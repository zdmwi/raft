import RaftNode.{ID, RaftCommand, RaftEvent}
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}

import scala.collection.immutable.Queue
import scala.concurrent.duration.DurationInt
import scala.util.Random

object RaftClient {
  sealed trait ClientEvent
  private case class ResponseTimeout(rpc: Any) extends ClientEvent
}
case class RaftClient(id: ID, cluster: Seq[(ID, ActorRef[RaftEvent])]) {
  import RaftClient.*
  import RaftNode.*

  private val tag = s"[Client $id]"
  // randomly choose a node from the cluster as the leader
  private var leaderHint: Option[ActorRef[RaftEvent]] = None
  private var leaderAddr: ActorRef[RaftEvent] = Random.shuffle(cluster).head._2

  // select the node with the matching id to be the favored unstable read replica
  private val readReplica = cluster(id)._2

  private var request: Option[RaftEvent] = None
  private var recipient: Option[ActorRef[RaftEvent]] = None

  private var sequenceNum = 0L

  def apply(): Behavior[String | RaftEvent | ClientEvent] = Behaviors.setup { context =>
    context.log.info("Client was created")

    context.log.info("Registering with the Raft cluster...")
    // Client must register with the cluster
    request = Some(RegisterClientRPC(id, sequenceNum, context.self)) // if just registering sequence num must always be 0
    leaderAddr ! request.get

    Behaviors.withTimers { timers =>

      val delay = 1
      timers.startTimerWithFixedDelay(ResponseTimeout(request), delay.seconds)

      Behaviors.receive { (context, msg) =>
        msg match {
          case cmd: String =>
            // parse the values
            val params = cmd.split(" ")
            val argument = params.lift(1)

            var op: RaftCommand = NoOp()
            context.log.trace(s"$tag: Sending request...")

            recipient = Some(leaderAddr)
            // parse the command and forward it to the cluster
            if cmd.startsWith("read") then
              context.log.info(s"$tag: Requesting read...")
              request = Some(ClientQueryRPC(ReadOp(), context.self))
            else if cmd.startsWith("unread") then
              context.log.info(s"$tag: Requesting unstable read...")
              request = Some(UnstableClientQueryRPC(ReadOp(), context.self))
              recipient = Some(readReplica)
            else if cmd.startsWith("buy") then
              context.log.info(s"$tag: Buying ${argument.get} tickets...")
              op = BuyOp(argument.get.toInt, sequenceNum, id)
              request = Some(ClientRequestRPC(id, sequenceNum, op, context.self))
            else if cmd.startsWith("replenish") then
              context.log.info(s"$tag: Replenishing ${argument.get} tickets...")
              op = ReplenishOp(argument.get.toInt, sequenceNum, id)
              request = Some(ClientRequestRPC(id, sequenceNum, op, context.self))
            else
              op = NoOp()
              request = Some(ClientRequestRPC(id, sequenceNum, op, context.self))

            recipient.get ! request.get
            timers.startTimerWithFixedDelay(ResponseTimeout(request), delay.seconds)
            Behaviors.same
          case rpc: ClientQueryResponseRPC =>
            if rpc.status == ClientRPCStatus.OK then
              context.log.info(s"$tag: Received read response: ${rpc.response}")
              timers.cancelAll()
              leaderHint = rpc.leaderHint
              request = None
            else if rpc.status == ClientRPCStatus.NOT_LEADER then
              if rpc.leaderHint.isDefined then
                leaderHint = rpc.leaderHint
            Behaviors.same
          case rpc: UnstableClientQueryResponseRPC =>
            context.log.info(s"$tag: Received unstable read response: ${rpc.response}")
            timers.cancelAll()
            leaderHint = rpc.leaderHint
            request = None
            Behaviors.same
          case rpc: ClientRequestResponseRPC =>
            if rpc.status == ClientRPCStatus.OK then
              timers.cancelAll()
              leaderHint = rpc.leaderHint
              request = None
              sequenceNum += 1
            Behaviors.same
          case rpc: RegisterClientResponseRPC =>
            if rpc.status == ClientRPCStatus.NOT_LEADER then
              if rpc.leaderHint.isDefined then
                leaderHint = rpc.leaderHint
            else if rpc.status == ClientRPCStatus.OK then
              timers.cancelAll()
              leaderAddr = rpc.leaderHint.get
              request = None
              sequenceNum += 1
              context.log.info(s"$tag: Registered successfully")
            Behaviors.same
          case _: ResponseTimeout =>
            // retry the rpc sent before
            // try another random node if we don't have a hint
            if leaderHint.isEmpty then
              val clusterWithoutUnresponsiveNode = cluster.filter(_._2 != leaderAddr)
              leaderAddr = Random.shuffle(clusterWithoutUnresponsiveNode).head._2
            else
              leaderAddr = leaderHint.get
              leaderHint = None

            context.log.info(s"$tag: Retrying request ${request.get} to $leaderAddr...")
            leaderAddr ! request.get
            Behaviors.same
          case _: RaftEvent =>
            Behaviors.same
        }
      }
    }
  }
}
