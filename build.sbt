name := "t-streams"

val tstreamsVersion = "1.0.3.1"

version := tstreamsVersion
organization := "com.bwsw"

scalaVersion := "2.11.8"

scalacOptions += "-feature"
scalacOptions += "-deprecation"

publishMavenStyle := true

licenses := Seq("Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0"))
homepage := Some(url("http://t-streams.com/"))

pomIncludeRepository := { _ => false }

pomExtra := (
    <scm>
      <url>git@github.com:bwsw/t-streams.git</url>
      <connection>scm:git@github.com:bwsw/t-streams.git</connection>
    </scm>
    <developers>
      <developer>
        <id>bitworks</id>
        <name>Bitworks Software, Ltd.</name>
        <url>http://bitworks.software/</url>
      </developer>
    </developers>)

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

publishArtifact in Test := false

//COMMON
libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.21",
  "org.slf4j" % "slf4j-simple" % "1.7.21",
  "org.scalatest" % "scalatest_2.11" % "3.0.0-M15",
  "org.scalamock" %% "scalamock-scalatest-support" % "3.2.2" % "test",
  "io.netty" % "netty-all" % "4.1.0.CR7",
  "com.aerospike" % "aerospike-client" % "3.2.1",
  "org.apache.commons" % "commons-collections4" % "4.1",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.6.3",
  "net.openhft" % "chronicle-queue" % "4.2.6",
  "org.scala-lang" % "scala-reflect" % "2.11.8",
  "org.scala-lang.modules" % "scala-xml_2.11" % "1.0.4",
  "org.cassandraunit" % "cassandra-unit" % "2.2.2.1",
  "log4j" % "log4j" % "1.2.17",
  "com.hazelcast" % "hazelcast" % "3.6.4",
  ("org.apache.zookeeper" % "zookeeper" % "3.4.6")
    .exclude("org.slf4j", "slf4j-log4j12"),
  "com.google.guava" % "guava" % "18.0")


libraryDependencies += ("com.datastax.cassandra" % "cassandra-driver-core" % "3.0.0")
  .exclude("com.google.guava", "guava")
  .exclude("io.netty", "netty-common")
  .exclude("io.netty", "netty-codec")
  .exclude("io.netty", "netty-transport")
  .exclude("io.netty", "netty-buffer")
  .exclude("io.netty", "netty-handler")

libraryDependencies += ("org.cassandraunit" % "cassandra-unit" % "2.2.2.1")
  .exclude("com.google.guava", "guava")
  .exclude("io.netty", "netty-common")
  .exclude("io.netty", "netty-codec")
  .exclude("io.netty", "netty-transport")
  .exclude("io.netty", "netty-buffer")
  .exclude("io.netty", "netty-handler")

//COORDINATION
resolvers += "twitter resolver" at "http://maven.twttr.com"
libraryDependencies += ("com.twitter.common.zookeeper" % "lock" % "0.0.38")
  .exclude("com.google.guava", "guava")
  .exclude("org.slf4f", "slf4j-api")
  .exclude("log4j", "log4j")
  .exclude("io.netty", "netty")
  .exclude("org.slf4j", "slf4j-log4j12")
  .exclude("org.apache.zookeeper", "zookeeper")

//ASSEMBLY STRATEGY
assemblyJarName in assembly := "t-streams-" + tstreamsVersion + ".jar"

assemblyMergeStrategy in assembly := {
  case PathList("org", "slf4j", "impl", xs@_*) => MergeStrategy.discard
  case PathList("org", "cassandraunit", xs@_*) => MergeStrategy.discard
  case PathList("org", "hamcrest", xs@_*) => MergeStrategy.discard
  case PathList("com", "twitter", "common", "zookeeper", xs@_*) => MergeStrategy.first
  case PathList("io", "netty", xs@_*) => MergeStrategy.first
  case PathList("com", "datastax", "cassandra", xs@_*) => MergeStrategy.first
  case PathList("org", "slf4j", xs@_*) => MergeStrategy.first
  case PathList("org", "scalatest", xs@_*) => MergeStrategy.first
  case PathList("com", "aerospike", xs@_*) => MergeStrategy.first
  case PathList("org", "apache", "commons", xs@_*) => MergeStrategy.first
  case PathList("com", "fasterxml", "jackson", "module", xs@_*) => MergeStrategy.first
  case PathList("net", "openhft", xs@_*) => MergeStrategy.first
  case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
}

//TESTS
parallelExecution in ThisBuild := false