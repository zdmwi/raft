import RaftNode.{ID, RaftEvent}
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}

import scala.concurrent.duration.DurationInt
import scala.util.Random

object RaftClient {
  sealed trait ClientEvent
  private case class ResponseTimeout(rpc: Any) extends ClientEvent
}
case class RaftClient(cluster: Seq[(ID, ActorRef[RaftEvent])]) {
  import RaftClient.*
  import RaftNode.*

  private val tag = "[Client]"
  // randomly choose a node from the cluster as the leader
  private var leaderHint: Option[ActorRef[RaftEvent]] = None
  private var leaderAddr: ActorRef[RaftEvent] = Random.shuffle(cluster).head._2

  private var clientId: Option[ID] = None

  private var request: Option[RaftEvent] = None

  private var sequenceNum = 0L

  def apply(): Behavior[String | RaftEvent | ClientEvent] = Behaviors.setup { context =>
    context.log.info("Client was created")

    context.log.info("Registering with the Raft cluster...")
    // Client must register with the cluster
    request = Some(RegisterClientRPC(sequenceNum, context.self)) // if just registering sequence num must always be 0
    leaderAddr ! request.get

    Behaviors.withTimers { timers =>

      val delay = 1
      timers.startTimerWithFixedDelay(ResponseTimeout(request), delay.seconds)

      Behaviors.receive { (context, msg) =>
        msg match {
          case cmd: String =>
            var op: RaftCommand = NoOp()
            if clientId.isDefined then
              context.log.info(s"$tag: Sending request...")
              // parse the command and forward it to the cluster
              if cmd.startsWith("read") then
                context.log.info(s"$tag: Requesting read of key...")
//                request = Some(ClientQueryRPC())
              else
                if cmd.startsWith("create") then
                  op = CreateOp(sequenceNum, clientId.get)
                else if cmd.startsWith("delete") then
                  op = DeleteOp(sequenceNum, clientId.get)
                else if cmd.startsWith("update") then
                  op = UpdateOp(sequenceNum, clientId.get)
                else
                  op = NoOp()
                request = Some(ClientRequestRPC(clientId.get, sequenceNum, op, context.self))

              leaderAddr ! request.get
              timers.startTimerWithFixedDelay(ResponseTimeout(request), delay.seconds)
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
              clientId = Some(rpc.clientId)
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
