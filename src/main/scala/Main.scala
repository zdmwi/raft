import RaftNode.*
import akka.actor.typed.ActorSystem

import scala.io.StdIn.readLine

@main def main(numNodes: Int): Unit = {
  val cluster = for (i <- 0 until numNodes) yield (i, ActorSystem(RaftNode(i)(), s"N$i"))

  cluster.foreach { (id, node) =>
    node ! Init(cluster.filter(_._1 != id))
  }

  val frontends = for (i <- 0 until numNodes) yield ActorSystem(RaftClient(i, cluster)(), s"C$i")

  val workloads = Map(
    1 -> Seq("buy 1", "buy 2", "buy 1", "unread", "read"),
    2 -> Seq("buy 1", "buy 2", "buy 3"),
    3 -> Seq("buy 4", "buy 1", "buy 1"),
    4 -> Seq("buy 2", "buy 1", "buy 2")
  )

  var done = false
  while !done do
    val input = readLine()
    if input.isEmpty then
      cluster.foreach { (_, node) =>
        node.terminate()
      }

      frontends.foreach { _.terminate() }
      done = true
    else
      var cmd: RaftEvent = NoOp()

      if input.startsWith("kill") then
        val id = input.split(" ")(1).toInt
        cmd = KillOp()
        cluster(id)._2 ! cmd
      else if input.startsWith("start") then
        val id = input.split(" ")(1).toInt
        cmd = Init(cluster.filter(_._1 != id))
        cluster(id)._2 ! cmd
      else if input.startsWith("workload") then
        // send requests
        for (id, requests) <- workloads do {
          requests.foreach { request => frontends(id) ! request }
        }
      else
        // run all the requests
        frontends(0) ! input
}
