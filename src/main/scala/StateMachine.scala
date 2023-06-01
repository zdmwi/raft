import RaftNode.{ID, LogEntry, RaftCommand, RaftEvent}
import akka.actor.typed.scaladsl.ActorContext

import scala.util.control.Breaks

case class NoOp() extends RaftCommand
case class RegisterOp(sequenceNum: Long, clientId: ID) extends RaftCommand
case class KillOp() extends RaftCommand
case class BuyOp(count: Int, sequenceNum: Long, clientId: ID) extends RaftCommand
case class ReplenishOp(count: Int, sequenceNum: Long, clientId: ID) extends RaftCommand
case class ReadOp() extends RaftCommand

class StateMachine(nodeId: ID, shouldPersist: Boolean = true, verbose: Boolean = true) {
  private val storageFilename = s"DB$nodeId.data"

  // for tracking a client and their sequence number
  private type Session = Map[Long, Option[RaftEvent]]
  private var clients = Map.empty[ID, Session]

  private val defaultNumTickets = 20
  private var ticketsRemaining: Int = {
    val storeOnDisk = StorageManager.deserialize[Int](storageFilename)
    storeOnDisk match {
      case Some(s) =>
        s
      case None =>
        defaultNumTickets
    }
  }

  private def updateStore(newTicketCount: Int): Unit =
    ticketsRemaining = newTicketCount
    if shouldPersist then
      StorageManager.serialize(ticketsRemaining, storageFilename)

  def simulate(tag: String, context: ActorContext[RaftEvent], log: Seq[LogEntry]): Unit =
    // reset the state
    ticketsRemaining = defaultNumTickets
    log.foreach { entry =>
      applyIfNotProcessed(tag, context, entry._2)
    }

  def getStore: Int = ticketsRemaining

  def setStore(newTicketCount: Int): Unit =
    ticketsRemaining = newTicketCount

  def getClients: Map[ID, Session] = clients

  def setClients(c: Map[ID, Session]): Unit =
    clients = c

  def applyIfNotProcessed(tag: String, context: ActorContext[RaftEvent], cmd: RaftCommand): Option[Int | RaftEvent]  =
    cmd match
      // we have to do if statements for all of these operations. Not ideal. Fix the typing
     case op: RegisterOp =>
       if clients.contains(op.clientId) && clients(op.clientId).contains(op.sequenceNum) then
         if verbose then
           context.log.info(s"$tag: Register request already processed. Continuing without applying...")
         clients(op.clientId)(op.sequenceNum)
       else
         clients = clients.updated(op.clientId, Map(0L -> None))
         context.log.info(s"$tag: Register operation applied. Clients: $clients")
         Some(op.clientId)
     case op: ReplenishOp =>
       if clients.contains(op.clientId) && clients(op.clientId).contains(op.sequenceNum) then
         if verbose then
           context.log.info(s"$tag: Replenish request already processed. Continued without applying...")
         clients(op.clientId)(op.sequenceNum)
       else
         if verbose then
           context.log.info(s"$tag: Adding ${op.count} tickets to remaining tickets...")
         updateStore(ticketsRemaining + op.count)
         None
     case op: BuyOp =>
       if clients.contains(op.clientId) && clients(op.clientId).contains(op.sequenceNum) then
         if verbose then
           context.log.info(s"$tag: Buy request already processed. Continuing without applying...")
         clients(op.clientId)(op.sequenceNum)
       else
         if verbose then
           context.log.info(s"$tag: Buying ${op.count} tickets...")

         if ticketsRemaining < op.count then
           context.log.info(s"$tag: Enough tickets are not available. Failed to complete purchase...")
         else
           context.log.trace(s"$tag: Successfully bought ${op.count} tickets...")
           updateStore(ticketsRemaining - op.count)
         None
     case _: ReadOp =>
       if verbose then
         context.log.info(s"$tag: Reading remaining tickets count...")
       Some(ticketsRemaining)
     case _: NoOp =>
       None
}
