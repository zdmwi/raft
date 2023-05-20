import RaftNode.ID
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}

import scala.concurrent.duration.DurationInt
import scala.util.Random
import scala.util.control.Breaks


enum ClientRPCStatus:
  case OK, NOT_LEADER, SESSION_EXPIRED

object ElectionTimeoutRange {
  val START = 150
  val END = 301
}

object RaftNode {
  private type Term = Long
  type ID = Int
  private type LogEntry = (Term, RaftCommand)

  sealed trait RaftEvent

  case class Init(nodes: Seq[(ID, ActorRef[RaftEvent])]) extends RaftEvent

  private case class ElectionTimeout() extends RaftEvent

  private case class HeartbeatTimeout() extends RaftEvent

  // Core Raft consensus RPCs and Responses
  sealed trait RPC extends RaftEvent

  private case class RequestVoteRPC(term: Term, candidateId: ID,
                                    lastLogIndex: Int, lastLogTerm: Term,
                                    logLength: Int, replyTo: ActorRef[RaftEvent]) extends RPC

  private case class RequestVoteResponseRPC(term: Term, voteGranted: Boolean) extends RPC

  private case class AppendEntriesRPC(term: Term, leaderId: ID,
                                      prevLogIndex: Int, prevLogTerm: Term,
                                      entries: List[LogEntry], leaderCommit: Int,
                                      replyTo: ActorRef[RaftEvent]) extends RPC

  private case class AppendEntriesResponseRPC(term: Term, success: Boolean, followerId: ID) extends RPC

  // Client interaction RPCs
  sealed trait ClientRPC extends RPC

  case class RegisterClientRPC(replyTo: ActorRef[RaftEvent]) extends ClientRPC

  case class RegisterClientResponseRPC(status: ClientRPCStatus, clientId: ID, leaderHint: Option[ActorRef[RaftEvent]]) extends ClientRPC

  trait RaftCommand extends RaftEvent
}

case class RaftNode(id: ID) {

  import RaftNode._

  private val tag = s"[Node $id]"
  private var nodes = List.empty[(ID, ActorRef[RaftEvent])]

  // Persistent state
  private var currentTerm: Term = 0L // initialized to 0 on first boot
  private var votedFor: Option[ID] = None // initialized to None
  private var log: List[LogEntry] = List.empty // Maybe include a NoOp so the first index is 1 ?

  // Volatile state
  private var commitIndex = 0
  private var lastApplied = 0

  private var leaderHint: Option[ActorRef[RaftEvent]] = None

  private def tryApplyCommit(context: ActorContext[RaftEvent]): Unit = {
    if commitIndex > lastApplied then
      lastApplied += 1
      val command = log(lastApplied - 1)
      context.log.info(s"$tag: Applying command $command to state machine...")
    // apply to state machine
  }

  def apply(): Behavior[RaftEvent] = Behaviors.setup { _ =>
    Behaviors.receive { (context, msg) =>
      msg match {
        case init: Init =>
          nodes = init.nodes.toList
          context.log.info(s"$tag: Becoming a follower...")
          becomeFollower()
        case _: RaftEvent =>
          Behaviors.same
      }
    }
  }

  private def becomeFollower(): Behavior[RaftEvent] = Behaviors.setup { context =>
    context.log.info(s"$tag: Became a follower")

    Behaviors.withTimers { timers =>
      // start the election timeout timer
      val timeout = Random.between(ElectionTimeoutRange.START, ElectionTimeoutRange.END)
      timers.startSingleTimer(ElectionTimeout(), timeout.milliseconds)

      Behaviors.receive { (context, msg) =>
        msg match {
          case rpc: RegisterClientRPC =>
            // if we get any messages from a client, redirect them to the leader or who we think it is at least
            context.log.info(s"$tag: Received request from client. Redirecting to leader...")
            rpc.replyTo ! RegisterClientResponseRPC(ClientRPCStatus.NOT_LEADER, -1, leaderHint)
            Behaviors.same
          case rpc: RequestVoteRPC =>
            tryApplyCommit(context)

            if rpc.term > currentTerm then
              context.log.info(s"$tag: Out of date. Updating term...")
              currentTerm = rpc.term
              votedFor = None

            val isNullOrCandidateId = votedFor.isEmpty || (votedFor.get == rpc.candidateId)

            val lastLogIndex = log.length - 1
            val lastLogTerm = log.lift(lastLogIndex).map(_._1).getOrElse(0L)

            var logAtLeastUpToDate = false
            if rpc.lastLogIndex - 1 == lastLogIndex && rpc.lastLogTerm != lastLogTerm then
            // if log indices match and the terms are different, then the node with the higher
            // log term is more up to date
              if rpc.lastLogTerm >= lastLogTerm then
                logAtLeastUpToDate = true

            else if rpc.lastLogIndex - 1 == lastLogIndex && rpc.lastLogTerm == lastLogTerm then
            // if log indices match and the terms also match, then the node with the longer
            // log is more up to date
              if rpc.logLength >= log.length then
                logAtLeastUpToDate = true

            context.log.info(s"$tag: candidateLastLogIndex=${rpc.lastLogIndex}, candidateLastLogTerm=${rpc.lastLogTerm}, candidateLastLogLength=${rpc.logLength} " +
              s"followerLastLogIndex=$lastLogIndex, followerLastLogTerm=$lastLogTerm, followerLogLength=${log.length}")

            context.log.info(s"$tag: isNullOrCandidateId=$isNullOrCandidateId, logAtleastUpToDate=$logAtLeastUpToDate")
            if rpc.term < currentTerm then
              // candidate is out of date. reply false
              rpc.replyTo ! RequestVoteResponseRPC(currentTerm, voteGranted = false)
            else if isNullOrCandidateId && logAtLeastUpToDate then
              // grant our vote and reset election timeout
              context.log.info(s"$tag: Voted for [Node ${rpc.candidateId}]")
              rpc.replyTo ! RequestVoteResponseRPC(currentTerm, voteGranted = true)
              votedFor = Some(rpc.candidateId)

              val timeout = Random.between(ElectionTimeoutRange.START, ElectionTimeoutRange.END)
              timers.startSingleTimer(ElectionTimeout(), timeout.milliseconds)
            Behaviors.same
          case rpc: AppendEntriesRPC =>
            if rpc.term > currentTerm then
              context.log.info(s"$tag: Out of date. Updating term...")
              currentTerm = rpc.term
              votedFor = None // new term so reset who we voted for
              leaderHint = Some(rpc.replyTo)
              Behaviors.same // remain as a follower
            else if rpc.term < currentTerm then
              context.log.info(s"$log: AppendEntriesRPC is from a past term. Rejecting...")
              rpc.replyTo ! AppendEntriesResponseRPC(currentTerm, success = false, id)
              Behaviors.same
            else
              leaderHint = Some(rpc.replyTo)
              // reset the timer
              val timeout = Random.between(ElectionTimeoutRange.START, ElectionTimeoutRange.END)
              timers.startSingleTimer(ElectionTimeout(), timeout.milliseconds)

              // if log does not contain an entry at prevLogIndex whose term matches prevLogTerm
              // delete the existing entry and all that follow it
              rpc.entries.zipWithIndex.foreach { (entry, idx) =>
                val realIdx = rpc.prevLogIndex + idx

                if log.isDefinedAt(realIdx) && log(realIdx)._1 != entry._1 then
                  context.log.info(s"$tag: Log entries do not match. Reconciling...")
                  log = log.take(realIdx)
              }

              // append any new entries not already in the log
              rpc.entries.zipWithIndex.foreach { (entry, idx) =>
                val realIdx = rpc.prevLogIndex + idx

                if !log.isDefinedAt(realIdx) then
                  context.log.info(s"$tag: Appending $entry to log...")
                  log = log.appended(entry)
                  context.log.info(s"$tag: Log $log")
              }

              // try to update commitIndex
              if rpc.leaderCommit > commitIndex then
                val indexOfLastNewEntry = log.length
                commitIndex = math.min(rpc.leaderCommit, indexOfLastNewEntry)

              val isInitialLog = rpc.prevLogIndex == 0 && rpc.prevLogTerm == 0L
              if isInitialLog || log.isDefinedAt(rpc.prevLogIndex - 1) && log(rpc.prevLogIndex - 1)._1 == rpc.prevLogTerm then
                context.log.trace(s"$tag: Replying true since prevLogIndex and prevLogTerm match...")
                rpc.replyTo ! AppendEntriesResponseRPC(currentTerm, success = true, id)
              else
                context.log.info(s"$tag: Not the same. Log: $log, prevLogIndex=${rpc.prevLogIndex - 1}, prevLogTerm=${rpc.prevLogTerm}")
              Behaviors.same
          case _: ElectionTimeout =>
            tryApplyCommit(context)

            context.log.info(s"$tag: Election timeout occurred. Becoming a candidate...")
            timers.cancelAll()
            becomeCandidate()
          case _: RaftEvent =>
            Behaviors.same
        }
      }
    }
  }

  private def becomeCandidate(): Behavior[RaftEvent] = Behaviors.setup { context =>
    context.log.info(s"$tag: Became candidate")
    // increment the current term
    currentTerm += 1

    // vote for ourself
    var votes = 1
    votedFor = Some(id)
    leaderHint = None

    Behaviors.withTimers { timers =>
      // reset the election timer
      val timeout = Random.between(ElectionTimeoutRange.START, ElectionTimeoutRange.END)
      timers.startSingleTimer(ElectionTimeout(), timeout.milliseconds)

      // Request votes from the other nodes
      context.log.info(s"$tag: Requesting votes...")
      val lastLogIndex = log.length
      val lastLogTerm = log.lastOption.map(_._1).getOrElse(0L)
      nodes.foreach { (nodeId, node) =>
        val rpc = RequestVoteRPC(currentTerm, id, lastLogIndex, lastLogTerm, log.length, context.self)
        context.log.info(s"$tag: Sending RequestVoteRPC $rpc to [Node $nodeId)}")
        node ! rpc
      }

      Behaviors.receive { (context, msg) =>
        msg match {
          case rpc: RegisterClientRPC =>
            context.log.info(s"$tag: Received request from client. Redirecting to leader...")
            rpc.replyTo ! RegisterClientResponseRPC(ClientRPCStatus.NOT_LEADER, -1, leaderHint)
            Behaviors.same
          case rpc: AppendEntriesRPC =>
            tryApplyCommit(context)

            if rpc.term >= currentTerm then
              context.log.info(s"$tag: Received AppendEntriesRPC from existing Leader [Node ${rpc.leaderId}]. Reverting to follower...")
              leaderHint = Some(rpc.replyTo)

              if rpc.term > currentTerm then
                currentTerm = rpc.term
                votedFor = None

              timers.cancelAll()
              becomeFollower()
            else
              // reject the RPC since it's from an older term
              rpc.replyTo ! AppendEntriesResponseRPC(currentTerm, success = false, id)
              Behaviors.same
          case rpc: RequestVoteRPC =>
            tryApplyCommit(context)

            // check if the term is out of date
            if rpc.term > currentTerm then
              currentTerm = rpc.term
              votedFor = None
              becomeFollower()
            else
              // reply false since a candidate will only ever vote for itself
              rpc.replyTo ! RequestVoteResponseRPC(currentTerm, voteGranted = false)
              Behaviors.same
          case rpc: RequestVoteResponseRPC =>
            tryApplyCommit(context)

            // check if the term is out of date
            if rpc.term > currentTerm then
              currentTerm = rpc.term
              votedFor = None
              becomeFollower()
            else
              if rpc.voteGranted then
                votes += 1

              val majority = math.ceil((nodes.length + 1) / 2)
              if votes > majority then
                // the election was won
                context.log.info(s"$tag: Won the election with $votes/${nodes.length + 1} votes. Becoming leader...")
                timers.cancelAll()
                becomeLeader()
              else
                Behaviors.same
          case _: ElectionTimeout =>
            tryApplyCommit(context)
            context.log.info(s"$tag: Election timeout occurred after receiving $votes/${nodes.length + 1}. Increasing term and restarting election...")
            timers.cancelAll()
            becomeCandidate()
          case _: RaftEvent =>
            Behaviors.same
        }
      }
    }
  }

  private def becomeLeader(): Behavior[RaftEvent] = Behaviors.setup { context =>
    context.log.info(s"$tag: Became leader")

    leaderHint = Some(context.self)

    // Volatile state
    val nextIndex: Array[Int] = Array.fill(nodes.length + 1) {
      log.length + 1
    }
    val matchIndex: Array[Int] = Array.fill(nodes.length + 1) {
      0
    }

    // book keeping state
    val prevIndexEntriesSent: Array[(Int, List[LogEntry])] = Array.fill(nodes.length + 1) {
      (0, List.empty[LogEntry])
    }

    var needsToCommit = false
    var replicatedCount = 0

    // client interaction requirement, upon becoming leader append NoOp entry to log
    context.log.info(s"$tag: Appending NoOp to log...")
    log = log.appended((currentTerm, NoOp()))
    context.log.info(s"$tag: Log is $log")

    // track state of replications so far while we need to commit
    needsToCommit = true
    replicatedCount += 1

    // register operation book keeping

    def tryUpdateCommitIndex(context: ActorContext[RaftEvent]): Unit = {
      var N = -1
      val breakCtl = Breaks()

      breakCtl.breakable {
        for (_, idx) <- log.zipWithIndex.reverse do
          if log(idx)._1 == currentTerm then
            N = idx + 1 // since we are using 1-indexing
            breakCtl.break
      }

      if N != -1 then
        var count = 1 // for us
        nodes.foreach { node =>
          if node._1 != id && matchIndex(node._1) >= N then
            count += 1
        }

        val majority = math.ceil((nodes.length + 1) / 2)
        if count > majority then
          context.log.trace(s"$tag: Updating commitIndex to $N")
          commitIndex = N
    }

    def sendEntries(node: (ID, ActorRef[RaftEvent])): Unit = {
      val nodeId = node._1
      context.log.trace(s"$tag: NextIndex=${nextIndex.mkString(",")}, ${nextIndex(nodeId)}")
      val prevLogIndex = nextIndex(nodeId) - 1
      val prevLogTerm = log.lift(prevLogIndex - 1).map(_._1).getOrElse(0L)

      val entries = log.slice(nextIndex(nodeId) - 1, log.length + 1)
      val rpc = AppendEntriesRPC(currentTerm, id, prevLogIndex, prevLogTerm, entries, commitIndex, context.self)
      context.log.trace(s"$tag: Sending AppendEntriesRPC $rpc to [Node $nodeId] ${node._2}...")
      node._2 ! rpc

      // keep track of the previous index and entries sent for updating the match matchIndex
      prevIndexEntriesSent(nodeId) = (prevLogIndex, entries)
    }

    Behaviors.withTimers { timers =>
      val timeout = ElectionTimeoutRange.START / 10
      timers.startTimerAtFixedRate(HeartbeatTimeout(), timeout.milliseconds)

      // upon election: send initial empty AppendEntries RPCs to each server
      nodes.foreach {
        sendEntries
      }

      Behaviors.receive { (context, msg) =>
        msg match {
          case rpc: RegisterClientRPC =>
            context.log.info(s"$tag: Received RegisterClientRPC from client. Processing...")
            log = log.appended((currentTerm, RegisterOp(log.length)))
            needsToCommit = true
            Behaviors.same
          case rpc: RequestVoteRPC =>
            tryApplyCommit(context)
            tryUpdateCommitIndex(context)

            if rpc.term > currentTerm then
              currentTerm = rpc.term
              votedFor = None
              timers.cancelAll()
              becomeFollower()
            else
              // votedFor for the leader will always be its own id so we always reply false here
              rpc.replyTo ! RequestVoteResponseRPC(currentTerm, voteGranted = false)
              Behaviors.same
          case rpc: AppendEntriesResponseRPC =>
            tryApplyCommit(context)
            tryUpdateCommitIndex(context)

            if rpc.term > currentTerm then
              context.log.info(s"$tag: Out of date. Updating currentTerm and reverting to a follower...")
              currentTerm = rpc.term
              votedFor = None
              timers.cancelAll()
              becomeFollower()
            else
              if rpc.success then
                // update nextIndex and matchIndex for follower
                val indexAndEntry = prevIndexEntriesSent(rpc.followerId)
                val prevIndex = indexAndEntry._1
                val prevEntries = indexAndEntry._2
                context.log.trace(s"$tag: MatchIndex before update - ${matchIndex.mkString(",")}")
                context.log.trace(s"$tag: NextIndex before update - ${nextIndex.mkString(",")}")
                matchIndex(rpc.followerId) = prevIndex + prevEntries.length

                if matchIndex(rpc.followerId) >= nextIndex(rpc.followerId) then
                  nextIndex(rpc.followerId) = matchIndex(rpc.followerId) + 1

                context.log.trace(s"$tag: MatchIndex after update - ${matchIndex.mkString(",")}")
                context.log.trace(s"$tag: NextIndex after update - ${nextIndex.mkString(",")}")

                if needsToCommit then
                  replicatedCount += 1

                val majority = math.ceil((nodes.length + 1) / 2)
                if replicatedCount > majority then
                  context.log.info(s"$tag: $replicatedCount/${nodes.length + 1} have replicated the entry. It is safe to commit.")
                  // it is safe to commit so reset the count and try to update the commitIndex
                  replicatedCount = 0
                  needsToCommit = false
                  tryUpdateCommitIndex(context)
              else
              // decrement nextIndex. RPC will be retried on the next heartbeat
                nextIndex(rpc.followerId) -= 1
              Behaviors.same
          case _: HeartbeatTimeout =>
            tryApplyCommit(context)
            tryUpdateCommitIndex(context)

            nodes.foreach {
              sendEntries
            }
            Behaviors.same
          case _: KillOp =>
            timers.cancelAll()
            apply()
          case _: RaftEvent =>
            // act as a sink for messages that are no longer important
            Behaviors.same
        }
      }
    }
  }
}
