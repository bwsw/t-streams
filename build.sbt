import sbt.Keys._
import sbtassembly.AssemblyPlugin.autoImport._


val tstreamsVersion = "2.2.5.1-SNAPSHOT"

// Common libraries
val commonDependencies = Seq(
  "org.slf4j" % "slf4j-api" % "1.7.24",
  "org.slf4j" % "slf4j-log4j12" % "1.7.24",
  "org.scalatest" %% "scalatest" % "3.0.1",
  "org.scalamock" %% "scalamock-scalatest-support" % "3.5.0",
  "org.apache.curator" % "curator-recipes" % "2.12.0",
  "com.trueaccord.scalapb" %% "compilerplugin" % "0.6.0-pre3")

// com.bwsw dependencies
val bwDependencies = Seq(
  "com.bwsw" % "tstreams-transaction-server_2.12" % "1.3.6-SNAPSHOT")

val scalaSettings = Seq("-deprecation", "-feature")

val javaSettings = Seq("-Dsun.net.maxDatagramSockets=1000")

val assemblyStrategySettings = Seq(assemblyMergeStrategy in assembly := {
  case PathList("org", "slf4j", "impl", xs@_*) => MergeStrategy.discard
  case PathList("io", "netty", xs@_*) => MergeStrategy.first
  case PathList("org", "slf4j", xs@_*) => MergeStrategy.first
  case PathList("org", "scalatest", xs@_*) => MergeStrategy.discard
  case PathList("org", "scalamock", xs@_*) => MergeStrategy.discard
  case "log4j.properties" => MergeStrategy.concat
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
})

val buildSettings = Seq(
  name := "t-streams",
  version := tstreamsVersion,
  scalaVersion := "2.12.1",
  libraryDependencies ++= commonDependencies ++ bwDependencies,
  javaOptions ++= javaSettings,
  scalacOptions ++= scalaSettings,
  organization := "com.bwsw",
  publishMavenStyle := true,
  licenses := Seq("Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
  homepage := Some(url("http://t-streams.com/")),
  pomIncludeRepository := { _ => false },
  parallelExecution in ThisBuild := false, //tests property
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases"  at nexus + "service/local/staging/deploy/maven2")
  },
  PB.targets in Compile := Seq(
    scalapb.gen(singleLineToString = true) -> (sourceManaged in Compile).value
  ),
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
    ),

  resolvers ++= Seq("Sonatype OSS" at "https://oss.sonatype.org/service/local/staging/deploy/maven2",
    "Sonatype OSS snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
    "Twitter Repo" at "https://maven.twttr.com",
    "Oracle Maven2 Repo" at "http://download.oracle.com/maven"),

  publishArtifact in Test := false,
  fork := true

) ++ assemblyStrategySettings

lazy val root = Project("t-streams-root", file("."))
    .settings(
      buildSettings:_*
    )
