import sbtassembly.Plugin.AssemblyKeys._

assemblySettings

assemblyOption in assembly ~= {
  _.copy(includeScala = false)
}

name := "SparkStateMachine"

version := "1.0"

scalaVersion := "2.10.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.0.1" % "provided",
  "org.apache.spark" %% "spark-streaming" % "1.0.1" % "provided",
  "com.typesafe" % "config" % "1.2.1",
  "com.github.nscala-time" %% "nscala-time" % "1.4.0",
  "com.typesafe.play" %% "play-json" % "2.4.0-M1",
  "net.debasishg" %% "redisclient" % "2.13")

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/"
)
