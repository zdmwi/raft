import RaftNode.{ID, RaftCommand, RaftEvent}
import akka.actor.typed.scaladsl.ActorContext

case class NoOp() extends RaftCommand

case class RegisterOp(sequenceNum: Long, clientId: ID) extends RaftCommand

case class KillOp() extends RaftCommand
case class CreateOp(sequenceNum: Long, clientId: ID) extends RaftCommand
case class DeleteOp(sequenceNum: Long, clientId: ID) extends RaftCommand
case class UpdateOp(sequenceNum: Long, clientId: ID) extends RaftCommand
case class AppendOp(sequenceNum: Long, clientId: ID) extends RaftCommand

class StateMachine() {
  // for tracking a client and their sequence number
  private type Session = Map[Long, Option[RaftEvent]]
  private var clients = Map.empty[ID, Session]

  def applyIfNotProcessed(tag: String, context: ActorContext[RaftEvent], cmd: RaftCommand): Option[Any] =
    cmd match
      // we have to do if statements for all of these operations. Not ideal. Fix the typing
     case op: RegisterOp =>
       if clients.contains(op.clientId) && clients(op.clientId).contains(op.sequenceNum) then
         context.log.info(s"$tag: Register request already processed. Continuing without applying...")
         println(clients)
         clients(op.clientId)(op.sequenceNum)
       else
         clients = clients.updated(op.clientId, Map(0L -> None))
         println(clients)
         Some(op.clientId)
     case op: CreateOp =>
       if clients.contains(op.clientId) && clients(op.clientId).contains(op.sequenceNum) then
         context.log.info(s"$tag: Create request already processed. Continuing without applying...")
         clients(op.clientId)(op.sequenceNum)
       else
         context.log.info(s"$tag: Creating new entry in K/V store...")
         None
     case op: UpdateOp =>
       if clients.contains(op.clientId) && clients(op.clientId).contains(op.sequenceNum) then
         context.log.info(s"$tag: Update request already processed. Continuing without applying...")
         clients(op.clientId)(op.sequenceNum)
       else
         context.log.info(s"$tag: Updating entry in K/V store...")
         None
     case op: DeleteOp =>
       if clients.contains(op.clientId) && clients(op.clientId).contains(op.sequenceNum) then
         context.log.info(s"$tag: Delete request already processed. Continuing without applying...")
         clients(op.clientId)(op.sequenceNum)
       else
         context.log.info(s"$tag: Deleting entry from K/V store...")
         None
     case _: NoOp =>
       None
}
