name := "t-streams"

val tstreamsVersion = "2.2.2-SNAPSHOT"

version := tstreamsVersion
organization := "com.bwsw"

scalaVersion := "2.12.1"

scalacOptions += "-feature"
scalacOptions += "-deprecation"

publishMavenStyle := true

licenses := Seq("Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0"))
homepage := Some(url("http://t-streams.com/"))

resolvers ++= Seq("Sonatype OSS" at "https://oss.sonatype.org/service/local/staging/deploy/maven2",
  "Sonatype OSS snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "Twitter Repo" at "https://maven.twttr.com",
  "Oracle Maven2 Repo" at "http://download.oracle.com/maven")

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
    </developers>
  )

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

publishArtifact in Test := false

PB.targets in Compile := Seq(
  scalapb.gen(singleLineToString = true) -> (sourceManaged in Compile).value
)

// Common libraries
libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.24",
  "org.slf4j" % "slf4j-log4j12" % "1.7.24",
  "org.scalatest" % "scalatest_2.12" % "3.0.1",
  "org.scalamock" % "scalamock-scalatest-support_2.12" % "3.5.0",
  "org.scala-lang" % "scala-reflect" % "2.12.1",
  "org.scala-lang.modules" % "scala-xml_2.12" % "1.0.6",
  "org.apache.curator" % "curator-recipes" % "2.11.0",
  "com.trueaccord.scalapb" %% "compilerplugin" % "0.6.0-pre3")

// com.bwsw dependencies
libraryDependencies ++= Seq(
  "com.bwsw" % "tstreams-transaction-server_2.12" % "1.3.4-SNAPSHOT")

//ASSEMBLY STRATEGY
assemblyJarName in assembly := "t-streams-" + tstreamsVersion + ".jar"

assemblyMergeStrategy in assembly := {
  case PathList("org", "slf4j", "impl", xs@_*) => MergeStrategy.discard
  case PathList("org", "hamcrest", xs@_*) => MergeStrategy.discard
  case PathList("io", "netty", xs@_*) => MergeStrategy.first
  case PathList("org", "slf4j", xs@_*) => MergeStrategy.first
  case PathList("org", "scalatest", xs@_*) => MergeStrategy.first
  case PathList("net", "openhft", xs@_*) => MergeStrategy.first
  case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
}

// Tests configuration
parallelExecution in ThisBuild := false

javaOptions += "-Dsun.net.maxDatagramSockets=1000"

fork := true