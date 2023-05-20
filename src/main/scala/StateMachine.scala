import RaftNode.{ID, RaftCommand, RaftEvent}
import akka.actor.typed.scaladsl.ActorContext

import scala.util.control.Breaks

case class NoOp() extends RaftCommand
case class RegisterOp(sequenceNum: Long, clientId: ID) extends RaftCommand
case class KillOp() extends RaftCommand
case class CreateOp(key: String, value: String, sequenceNum: Long, clientId: ID) extends RaftCommand
case class UpdateOp(key: String, value: String, sequenceNum: Long, clientId: ID) extends RaftCommand
case class ReadOp(key: String) extends RaftCommand
case class DeleteOp(key: String, sequenceNum: Long, clientId: ID) extends RaftCommand

class StateMachine(nodeId: ID) {
  private val storageFilename = s"KVS$nodeId.data"

  // for tracking a client and their sequence number
  private type Session = Map[Long, Option[RaftEvent]]
  private var clients = Map.empty[ID, Session]

  private var store: Map[String, String] = {
    val storeOnDisk = StorageManager.deserialize[Map[String, String]](storageFilename)
    storeOnDisk match {
      case Some(s) =>
        s
      case None =>
        Map.empty
    }
  }

  private def updateStore(newStore: Map[String, String]): Unit =
    store = newStore
    StorageManager.serialize(store, storageFilename)

  def applyIfNotProcessed(tag: String, context: ActorContext[RaftEvent], cmd: RaftCommand): Option[String | Any]  =
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
         updateStore(store.updated(op.key, op.value))
         None
     case op: ReadOp =>
       context.log.info(s"$tag: Reading from entry ${op.key} K/V store")
       store.get(op.key)
     case op: UpdateOp =>
       if clients.contains(op.clientId) && clients(op.clientId).contains(op.sequenceNum) then
         context.log.info(s"$tag: Update request already processed. Continuing without applying...")
         clients(op.clientId)(op.sequenceNum)
       else
         context.log.info(s"$tag: Updating entry ${op.key} -> ${op.value} in K/V store...")
         updateStore(store.updated(op.key, op.value))
         None
     case op: DeleteOp =>
       if clients.contains(op.clientId) && clients(op.clientId).contains(op.sequenceNum) then
         context.log.info(s"$tag: Delete request already processed. Continuing without applying...")
         clients(op.clientId)(op.sequenceNum)
       else
         context.log.info(s"$tag: Deleting entry ${op.key} from K/V store...")
         updateStore(store.removed(op.key))
         None
     case _: NoOp =>
       None
}
