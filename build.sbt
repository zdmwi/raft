ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.2.2"

val akkaVersion = "2.8.2"

lazy val root = (
  project in file("."))
  .settings(
    name := "raft2",
    fork := true,
    run / connectInput := true,
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
      "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion,
      "org.scalatest" %% "scalatest" % "3.2.15" % Test,
      "ch.qos.logback" % "logback-classic" % "1.4.6"
    )

  )
