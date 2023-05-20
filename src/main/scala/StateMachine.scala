import RaftNode.{ID, RaftCommand, RaftEvent}

case class NoOp() extends RaftCommand

case class RegisterOp(clientId: ID) extends RaftCommand

case class KillOp() extends RaftCommand

case class StartOp() extends RaftCommand

case class AppendOp() extends RaftCommand

class StateMachine() {
  // for tracking a client and their sequence number
  private type Session = (ID, Option[RaftEvent])
  private var clients = Map.empty[ID, Session]

  def hasSession(clientID: ID): Boolean =
    clients.contains(clientID)

  def apply(cmd: RaftCommand): Option[Any] =
   cmd match
     case op: RegisterOp =>
       clients = clients.updated(op.clientId, (0, None))
       println(clients)
       Some(op.clientId)
     case op: AppendOp =>
       None
     case op: NoOp =>
       None
}
