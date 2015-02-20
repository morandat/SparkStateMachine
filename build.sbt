import sbtassembly.Plugin.AssemblyKeys._

assemblySettings

assemblyOption in assembly ~= {
  _.copy(includeScala = true)
}

name := "SparkStateMachine"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.2.1",
  "com.github.nscala-time" %% "nscala-time" % "1.4.0",

  "org.apache.hadoop" % "hadoop-client" % "2.2.0",

  "org.apache.spark" %% "spark-core" % "1.0.1" % "provided",
  "org.apache.spark" %% "spark-streaming" % "1.0.1" % "provided",
  "com.typesafe.play" %% "play-json" % "2.4.0-M1",
  "org.json4s" %% "json4s-jackson" % "3.2.11",
  "net.debasishg" %% "redisclient" % "2.13"
)

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/"
)

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
  case PathList("org", "apache", "commons", "beanutils", xs@_*) => MergeStrategy.last
  case PathList("org", "apache", "commons", "collections", xs@_*) => MergeStrategy.last
  case PathList("org", "apache", "hadoop", "yarn", xs@_*) => MergeStrategy.last
  case x => old(x)
}}
