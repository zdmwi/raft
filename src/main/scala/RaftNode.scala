import RaftNode.ID
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}

import scala.collection.immutable.Queue
import scala.concurrent.duration.DurationInt
import scala.util.Random
import scala.util.control.Breaks


enum ClientRPCStatus:
  case OK, NOT_LEADER

object ElectionTimeoutRange {
  val START = 150
  val END = 301
}

object RaftNode {
  private type Term = Long
  type ID = Int
  type LogEntry = (Term, RaftCommand)

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

  case class RegisterClientRPC(clientId: ID, sequenceNum: Long, replyTo: ActorRef[RaftEvent]) extends ClientRPC

  case class RegisterClientResponseRPC(status: ClientRPCStatus, leaderHint: Option[ActorRef[RaftEvent]]) extends ClientRPC

  case class ClientRequestRPC(clientId: ID, sequenceNum: Long, command: RaftCommand, replyTo: ActorRef[RaftEvent]) extends ClientRPC

  case class ClientRequestResponseRPC(status: ClientRPCStatus, response: Any, leaderHint: Option[ActorRef[RaftEvent]]) extends ClientRPC

  case class ClientQueryRPC(query: RaftCommand, replyTo: ActorRef[RaftEvent]) extends ClientRPC

  case class UnstableClientQueryRPC(query: RaftCommand, replyTo: ActorRef[RaftEvent]) extends ClientRPC

  case class ClientQueryResponseRPC(status: ClientRPCStatus, response: Any, leaderHint: Option[ActorRef[RaftEvent]]) extends ClientRPC

  case class UnstableClientQueryResponseRPC(status: ClientRPCStatus, response: Any, leaderHint: Option[ActorRef[RaftEvent]]) extends ClientRPC
  trait RaftCommand extends RaftEvent
}

case class RaftNode(id: ID) {
  import RaftNode.*

  private val currentTermFilename = s"N$id-ct.raft"
  private val votedForFilename = s"N$id-vf.raft"
  private val logFilename = s"N$id-log.raft"

  private val tag = s"[Node $id]"
  private var nodes = List.empty[(ID, ActorRef[RaftEvent])]

  // Persistent state
  private var currentTerm: Term = {
    val currentTermOnDisk = StorageManager.deserialize[Term](currentTermFilename)
    currentTermOnDisk match {
      case Some(term) =>
        term
      case None =>
        0L
    }
  }  // initialized to 0 on first boot
  private var votedFor: Option[ID] = {
    val votedForOnDisk = StorageManager.deserialize[Option[ID]](votedForFilename)
    votedForOnDisk match {
      case Some(vf) =>
        vf
      case None =>
        None
    }
  } // initialized to None
  private var log: List[LogEntry] = {
    val logOnDisk = StorageManager.deserialize[List[LogEntry]](logFilename)
    logOnDisk match {
      case Some(lg) =>
        lg
      case None =>
        List.empty[LogEntry]
    }
  }

  // Volatile state
  private var commitIndex = 0
  private var lastApplied = 0

  private var leaderHint: Option[ActorRef[RaftEvent]] = None

  private var stateMachine = StateMachine(id)
  private var tentativeStateMachine = StateMachine(id, false, false) // this state machine will have the state for uncommitted entries

  private var requestQueue = Queue.empty[(String, ActorRef[RaftEvent], Option[RaftEvent])]
  private def tryApplyCommit(context: ActorContext[RaftEvent]): Option[String | Any] = {
    if commitIndex > lastApplied then
      lastApplied += 1
      val command = log(lastApplied - 1)._2
      context.log.trace(s"$tag: Applying command $command to state machine...")
      return stateMachine.applyIfNotProcessed(tag, context, command)

    None
  }

  private def updateLog(context: ActorContext[RaftEvent], newLog: List[LogEntry]) =
    log = newLog
    context.log.trace(s"$tag: Updating tentative state...")
    tentativeStateMachine.simulate(tag, context, log)
    StorageManager.serialize(log, logFilename)

  private def updateCurrentTerm(newTerm: Term) =
    currentTerm = newTerm
    StorageManager.serialize(currentTerm, currentTermFilename)

  private def updateVotedFor(newVotedFor: Option[ID]) =
    votedFor = newVotedFor
    StorageManager.serialize(votedFor, votedForFilename)

  private def respondToUnstableReadRequest(context: ActorContext[RaftEvent], rpc: UnstableClientQueryRPC): Unit =
    // all nodes can respond to an unstable query so we try to service it with our tentative state
    context.log.info(s"$tag: Received $rpc from client. Reading from application...")

    // read the response from the tentative state
    if log.isEmpty then
      context.log.info(s"$tag: Log is empty. Indicating to client...")
      // we use a None response to indicate that the log is empty
      rpc.replyTo ! UnstableClientQueryResponseRPC(status = ClientRPCStatus.NOT_LEADER, None, leaderHint)
    else
      context.log.trace(s"$tag: Sending response to client from tentative state...")

      val result = tentativeStateMachine.applyIfNotProcessed(tag, context, rpc.query)
      rpc.replyTo ! UnstableClientQueryResponseRPC(ClientRPCStatus.NOT_LEADER, result, leaderHint)

  def apply(): Behavior[RaftEvent] = Behaviors.setup { _ =>
    commitIndex = 0
    lastApplied = 0
    leaderHint = None
    stateMachine = StateMachine(id)
    tentativeStateMachine = StateMachine(id, false, false)
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
            rpc.replyTo ! RegisterClientResponseRPC(ClientRPCStatus.NOT_LEADER, leaderHint)
            Behaviors.same
          case rpc: ClientRequestRPC =>
            // redirect them to who we think the leader is
            context.log.info(s"$tag: Received request from client. Redirecting to leader...")
            rpc.replyTo ! ClientRequestResponseRPC(ClientRPCStatus.NOT_LEADER, -1, leaderHint)
            Behaviors.same
          case rpc: ClientQueryRPC =>
            // redirect them to who we think the leader is
            context.log.info(s"$tag: Received request from client. Redirecting to leader...")
            rpc.replyTo ! ClientQueryResponseRPC(ClientRPCStatus.NOT_LEADER, -1, leaderHint)
            Behaviors.same
          case rpc: UnstableClientQueryRPC =>
            respondToUnstableReadRequest(context, rpc)
            Behaviors.same
          case rpc: RequestVoteRPC =>
            if rpc.term > currentTerm then
              context.log.info(s"$tag: Out of date. Updating term...")
              updateCurrentTerm(rpc.term)
              updateVotedFor(None)

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

            context.log.trace(s"$tag: candidateLastLogIndex=${rpc.lastLogIndex}, candidateLastLogTerm=${rpc.lastLogTerm}, candidateLastLogLength=${rpc.logLength} " +
              s"followerLastLogIndex=$lastLogIndex, followerLastLogTerm=$lastLogTerm, followerLogLength=${log.length}")

            context.log.trace(s"$tag: isNullOrCandidateId=$isNullOrCandidateId, logAtleastUpToDate=$logAtLeastUpToDate")
            if rpc.term < currentTerm then
              // candidate is out of date. reply false
              rpc.replyTo ! RequestVoteResponseRPC(currentTerm, voteGranted = false)
            else if isNullOrCandidateId && logAtLeastUpToDate then
              // grant our vote and reset election timeout
              context.log.info(s"$tag: Voted for [Node ${rpc.candidateId}]")
              rpc.replyTo ! RequestVoteResponseRPC(currentTerm, voteGranted = true)
              updateVotedFor(Some(rpc.candidateId))

              val timeout = Random.between(ElectionTimeoutRange.START, ElectionTimeoutRange.END)
              timers.startSingleTimer(ElectionTimeout(), timeout.milliseconds)
            Behaviors.same
          case rpc: AppendEntriesRPC =>
            if rpc.term > currentTerm then
              context.log.info(s"$tag: Out of date. Updating term...")
              updateCurrentTerm(rpc.term)
              updateVotedFor(None) // new term so we reset who we voted for
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

              if log.isDefinedAt(rpc.prevLogIndex - 1) && log(rpc.prevLogIndex - 1)._1 != rpc.prevLogTerm then
                rpc.replyTo ! AppendEntriesResponseRPC(currentTerm, success = false, id)
              else
                // if log does not contain an entry at prevLogIndex whose term matches prevLogTerm
                // delete the existing entry and all that follow it

                rpc.entries.zipWithIndex.foreach { (entry, idx) =>
                  val realIdx = rpc.prevLogIndex + idx

                  if log.isDefinedAt(realIdx) && log(realIdx)._1 != entry._1 then
                    context.log.info(s"$tag: Log entries do not match. Reconciling...")
                    updateLog(context, log.take(realIdx))
                }

                // append any new entries not already in the log
                rpc.entries.zipWithIndex.foreach { (entry, idx) =>
                  val realIdx = rpc.prevLogIndex + idx

                  if !log.isDefinedAt(realIdx) then
                    context.log.trace(s"$tag: Appending $entry to log...")
                    updateLog(context, log.appended(entry))
                    context.log.trace(s"$tag: Log $log")
                }

                // try to update commitIndex
                if rpc.leaderCommit > commitIndex then
                  val indexOfLastNewEntry = log.length
                  commitIndex = math.min(rpc.leaderCommit, indexOfLastNewEntry)

                tryApplyCommit(context)

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
          case _: KillOp =>
            timers.cancelAll()
            apply()
          case _: RaftEvent =>
            Behaviors.same
        }
      }
    }
  }

  private def becomeCandidate(): Behavior[RaftEvent] = Behaviors.setup { context =>
    context.log.info(s"$tag: Became candidate")
    // increment the current term
    updateCurrentTerm(currentTerm + 1)

    // vote for ourself
    var votes = 1
    updateVotedFor(Some(id))
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
        context.log.info(s"$tag: Sending RequestVoteRPC $rpc to [Node $nodeId]")
        node ! rpc
      }

      Behaviors.receive { (context, msg) =>
        msg match {
          case rpc: RegisterClientRPC =>
            context.log.info(s"$tag: Received request from client. Redirecting to leader...")
            rpc.replyTo ! RegisterClientResponseRPC(ClientRPCStatus.NOT_LEADER, leaderHint)
            Behaviors.same
          case rpc: ClientRequestRPC =>
            context.log.info(s"$tag: Received request from client. Redirecting to leader...")
            rpc.replyTo ! ClientRequestResponseRPC(ClientRPCStatus.NOT_LEADER, -1, leaderHint)
            Behaviors.same
          case rpc: ClientQueryRPC =>
            // redirect them to who we think the leader is
            context.log.info(s"$tag: Received request from client. Redirecting to leader...")
            rpc.replyTo ! ClientQueryResponseRPC(ClientRPCStatus.NOT_LEADER, -1, leaderHint)
            Behaviors.same
          case rpc: UnstableClientQueryRPC =>
            respondToUnstableReadRequest(context, rpc)
            Behaviors.same
          case rpc: AppendEntriesRPC =>
            tryApplyCommit(context)

            if rpc.term >= currentTerm then
              context.log.info(s"$tag: Received AppendEntriesRPC from existing Leader [Node ${rpc.leaderId}]. Reverting to follower...")
              leaderHint = Some(rpc.replyTo)

              if rpc.term > currentTerm then
                updateCurrentTerm(rpc.term)
                updateVotedFor(None)

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
              updateCurrentTerm(rpc.term)
              updateVotedFor(None)
              becomeFollower()
            else
              // reply false since a candidate will only ever vote for itself
              rpc.replyTo ! RequestVoteResponseRPC(currentTerm, voteGranted = false)
              Behaviors.same
          case rpc: RequestVoteResponseRPC =>
            tryApplyCommit(context)

            // check if the term is out of date
            if rpc.term > currentTerm then
              updateCurrentTerm(rpc.term)
              updateVotedFor(None)
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
          case _: KillOp =>
            timers.cancelAll()
            apply()
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

    var readIndex: Option[Int] = None
    var replicatedCount = 0

    // client interaction requirement, upon becoming leader append NoOp entry to log
    // this helps us to know which entries have been committed at the start of the leader's term
    context.log.trace(s"$tag: Appending NoOp to log...")
    updateLog(context, log.appended((currentTerm, NoOp())))
    context.log.trace(s"$tag: Log is $log")

    // track state of replications so far while we need to commit
    replicatedCount += 1

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
            val command = RegisterOp(rpc.sequenceNum, rpc.clientId)
            context.log.info(s"$tag: Received $rpc from client...")
            context.log.trace(s"$tag: Appending $command to log...")
            updateLog(context, log.appended((currentTerm, command)))
            context.log.trace(s"$tag: Log $log")

            // add the request to the request queue
            val res = RegisterClientResponseRPC(ClientRPCStatus.OK, leaderHint)
            requestQueue = requestQueue.enqueue(("register", rpc.replyTo, Some(res))).distinct

            Behaviors.same
          case rpc: ClientRequestRPC =>
            context.log.info(s"$tag: Received $rpc from client. Appending ${rpc.command} to log...")
            updateLog(context, log.appended((currentTerm, rpc.command)))
            context.log.trace(s"$tag: Log $log")

            val res = ClientRequestResponseRPC(ClientRPCStatus.OK, None, leaderHint)
            requestQueue = requestQueue.enqueue(("request", rpc.replyTo, Some(res))).distinct

            Behaviors.same
          case rpc: ClientQueryRPC =>
            context.log.info(s"$tag: Received $rpc from client. Reading from application...")

            // save the current commitIndex in a local variable called readIndex. This will be used
            // as the lower bound for the version of the state that the query operates against
            readIndex = Some(commitIndex)

            requestQueue = requestQueue.enqueue(("read", rpc.replyTo, None)).distinct
            Behaviors.same
          case rpc: UnstableClientQueryRPC =>
            respondToUnstableReadRequest(context, rpc)
            Behaviors.same
          case rpc: RequestVoteRPC =>
            tryApplyCommit(context)
            tryUpdateCommitIndex(context)

            if rpc.term > currentTerm then
              updateCurrentTerm(rpc.term)
              updateVotedFor(None)
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
              updateCurrentTerm(rpc.term)
              updateVotedFor(None)
              timers.cancelAll()
              becomeFollower()
            else
              if rpc.success then
                // update nextIndex and matchIndex for follower
                val indexAndEntry = prevIndexEntriesSent(rpc.followerId)
                val prevIndex = indexAndEntry._1
                val prevEntries = indexAndEntry._2
                matchIndex(rpc.followerId) = prevIndex + prevEntries.length

                if matchIndex(rpc.followerId) >= nextIndex(rpc.followerId) then
                  nextIndex(rpc.followerId) = matchIndex(rpc.followerId) + 1

                replicatedCount += 1

                val majority = math.ceil((nodes.length + 1) / 2)
                if replicatedCount > majority then
                  context.log.trace(s"$tag: $replicatedCount/${nodes.length + 1} have replicated the entry. It is safe to commit.")

                  // it is safe to commit so reset the count and try to update the commitIndex
                  tryUpdateCommitIndex(context)
                  tryApplyCommit(context)
                  replicatedCount = 0

                  if requestQueue.nonEmpty then
                    val (pendingRequest, updatedQueue) = requestQueue.dequeue
                    requestQueue = updatedQueue

                    val reqType = pendingRequest._1
                    val replyTo = pendingRequest._2
                    var res = pendingRequest._3

                    // if this is a read request, we know that we are still the leader since we received
                    // the majority of responses, therefore readIndex (set where we respond to ClientQueryRPC), was,
                    // at the time the largest commitIndex ever seen by any server in the cluster
                    if reqType == "read" then
                      // we wait for the state machine to advance at least as far as readIndex which is enough to satisfy
                      // linearizability
                      if lastApplied >= readIndex.get then
                        // finally read the state from the state machine and return the response
                        context.log.info(s"$tag: Sending read reply to $replyTo...")
                        val read = stateMachine.applyIfNotProcessed(tag, context, ReadOp())
                        res = Some(ClientQueryResponseRPC(ClientRPCStatus.OK, read, leaderHint))
                        replyTo ! res.get
                      else
                        // add it back to the queue
                        requestQueue = requestQueue.enqueue(pendingRequest).distinct
                    else
                      context.log.info(s"$tag: Sending request reply to $replyTo...")
                      replyTo ! res.get
                  else
                    // No requests then it is just a heartbeat
                    context.log.trace(s"$tag: No requests to respond to")
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
