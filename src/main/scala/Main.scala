import RaftNode.*
import akka.actor.typed.ActorSystem

import scala.io.StdIn.readLine

object Main {
  def main(args: Array[String]): Unit = {
    val numNodes = 3
    val cluster = for (i <- 0 until numNodes) yield (i, ActorSystem(RaftNode(i)(), s"N$i"))

    cluster.foreach { (id, node) =>
      node ! Init(cluster.filter(_._1 != id))
    }

    val client = ActorSystem(RaftClient(cluster)(), "Client")

    var done = false
    while !done do
      val input = readLine()
      if input.isEmpty then
        cluster.foreach { (_, node) =>
          node.terminate()
        }
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
        else
          println(input)
          client ! input
  }
}
