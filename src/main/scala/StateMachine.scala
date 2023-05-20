import RaftNode.{ID, RaftCommand, RaftEvent}
import akka.actor.typed.scaladsl.ActorContext

case class NoOp() extends RaftCommand
case class RegisterOp(sequenceNum: Long, clientId: ID) extends RaftCommand
case class KillOp() extends RaftCommand
case class CreateOp(key: String, value: String, sequenceNum: Long, clientId: ID) extends RaftCommand
case class UpdateOp(key: String, value: String, sequenceNum: Long, clientId: ID) extends RaftCommand
case class ReadOp(key: String) extends RaftCommand
case class DeleteOp(key: String, sequenceNum: Long, clientId: ID) extends RaftCommand

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
         clients(op.clientId)(op.sequenceNum)
       else
         clients = clients.updated(op.clientId, Map(0L -> None))
         context.log.info(s"$tag: Register operation applied. Clients: $clients")
         Some(op.clientId)
     case op: CreateOp =>
       if clients.contains(op.clientId) && clients(op.clientId).contains(op.sequenceNum) then
         context.log.info(s"$tag: Create request already processed. Continuing without applying...")
         clients(op.clientId)(op.sequenceNum)
       else
         context.log.info(s"$tag: Creating new entry ${op.key} -> ${op.value} in K/V store...")
         None
     case op: ReadOp =>
       context.log.info(s"$tag: Reading from entry ${op.key} K/V store")
       None
     case op: UpdateOp =>
       if clients.contains(op.clientId) && clients(op.clientId).contains(op.sequenceNum) then
         context.log.info(s"$tag: Update request already processed. Continuing without applying...")
         clients(op.clientId)(op.sequenceNum)
       else
         context.log.info(s"$tag: Updating entry ${op.key} -> ${op.value} in K/V store...")
         None
     case op: DeleteOp =>
       if clients.contains(op.clientId) && clients(op.clientId).contains(op.sequenceNum) then
         context.log.info(s"$tag: Delete request already processed. Continuing without applying...")
         clients(op.clientId)(op.sequenceNum)
       else
         context.log.info(s"$tag: Deleting entry ${op.key} from K/V store...")
         None
     case _: NoOp =>
       None
}
