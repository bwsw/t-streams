name := "t-streams"

version := "1.0"

scalaVersion := "2.11.8"

scalacOptions += "-feature"
scalacOptions += "-deprecation"


//COMMON
libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.21",
  "org.slf4j" % "slf4j-simple" % "1.7.21",
  "org.scalatest" % "scalatest_2.11" % "3.0.0-M15",
  "io.netty" % "netty-all" % "4.1.0.CR7",
  "com.aerospike" % "aerospike-client" % "3.2.1",
  "org.apache.commons" % "commons-collections4" % "4.1",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.6.3",
  "net.openhft" % "chronicle-queue" % "4.2.6",
  "org.scala-lang" % "scala-reflect" % "2.11.8",
  "org.scala-lang.modules" % "scala-xml_2.11" % "1.0.4",
  "log4j" % "log4j" % "1.2.17",
  ("org.apache.zookeeper" % "zookeeper" % "3.4.6")
    .exclude("org.slf4j","slf4j-log4j12"),
  "com.google.guava" % "guava" % "18.0")


libraryDependencies += ("com.datastax.cassandra" % "cassandra-driver-core" % "3.0.0")
  .exclude("com.google.guava","guava")
  .exclude("io.netty", "netty-common")
  .exclude("io.netty", "netty-codec")
  .exclude("io.netty", "netty-transport")
  .exclude("io.netty", "netty-buffer")
  .exclude("io.netty", "netty-handler")

//COORDINATION
resolvers += "twitter resolver" at "http://maven.twttr.com"
libraryDependencies += ("com.twitter.common.zookeeper" % "lock" % "0.0.38")
  .exclude("com.google.guava","guava")
  .exclude("org.slf4f", "slf4j-api")
  .exclude("log4j","log4j")
  .exclude("io.netty", "netty")
  .exclude("org.slf4j","slf4j-log4j12")
  .exclude("org.apache.zookeeper", "zookeeper")


//ASSEMBLY STRATEGY
assemblyJarName in assembly := "t-streams" + version.key.label + ".jar"

assemblyMergeStrategy in assembly := {
  case PathList("org","slf4f","slf4j-simple", xs @ _*) => MergeStrategy.discard
  case PathList("com","typesafe","akka", xs @ _*) => MergeStrategy.first
  case PathList("com","twitter","common","zookeeper", xs @ _*) => MergeStrategy.first
  case PathList("io", "netty", xs @ _*) => MergeStrategy.first
  case PathList("com", "datastax", "cassandra", xs @ _*) => MergeStrategy.first
  case PathList("org", "slf4j", xs @ _*) => MergeStrategy.first
  case PathList("org", "scalatest", xs @ _*) => MergeStrategy.first
  case PathList("com", "aerospike", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", "commons", xs @ _*) => MergeStrategy.first
  case PathList("com", "fasterxml","jackson","module", xs @ _*) => MergeStrategy.first
  case PathList("net", "openhft", xs @ _*) => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}


//TESTS
parallelExecution in ThisBuild := false