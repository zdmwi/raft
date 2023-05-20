import RaftNode.{ID, RaftEvent }
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}

import scala.concurrent.duration.DurationInt
import scala.util.Random

object RaftClient {
  sealed trait ClientEvent
  private case class ResponseTimeout(rpc: Any) extends ClientEvent
}
case class RaftClient(cluster: Seq[(ID, ActorRef[RaftEvent])]) {
  import RaftClient._
  import RaftNode._

  private val tag = "[Client]"
  // randomly choose a node from the cluster as the leader
  private var leaderAddr: ActorRef[RaftEvent] = Random.shuffle(cluster).head._2

  private var rpc: Option[RaftEvent] = None

  def apply(): Behavior[String | RaftEvent | ClientEvent] = Behaviors.setup { context =>
    context.log.info("Client was created")

    context.log.info("Registering with the Raft cluster...")
    // Client must register with the cluster
    rpc = Some(RegisterClientRPC(context.self))
    leaderAddr ! rpc.get

    Behaviors.withTimers { timers =>

      val delay = 1
      timers.startSingleTimer(ResponseTimeout(rpc), delay.seconds)

      Behaviors.receive { (context, msg) =>
        msg match {
          case cmd: String =>
            // parse the command and forward it to the cluster
            leaderAddr ! NoOp()
            Behaviors.same
          case rpc: RegisterClientResponseRPC =>
            if rpc.status == ClientRPCStatus.NOT_LEADER then
              if rpc.leaderHint.isEmpty then
              // randomly select another node
                leaderAddr = Random.shuffle(cluster).head._2
              else
              // use the hint we got from the response
                leaderAddr = rpc.leaderHint.get
              context.log.info(s"$tag: Retrying RegisterClientRPC")
            else if rpc.status == ClientRPCStatus.OK then
              timers.cancelAll()
              context.log.info(s"$tag: Registered successfully")
            Behaviors.same
          case _: ResponseTimeout =>
            // retry the rpc sent before
            leaderAddr ! rpc.get
            Behaviors.same
          case _: RaftEvent =>
            Behaviors.same
        }
      }
    }
  }
}
