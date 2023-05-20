import RaftNode.{ID, RaftCommand}

case class NoOp() extends RaftCommand

case class RegisterOp(clientId: ID) extends RaftCommand

case class KillOp() extends RaftCommand

case class StartOp() extends RaftCommand

class StateMachine() {
}
