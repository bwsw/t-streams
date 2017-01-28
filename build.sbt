name := "t-streams"

val tstreamsVersion = "2.0.1-SNAPSHOT"

version := tstreamsVersion
organization := "com.bwsw"

scalaVersion := "2.12.1"

scalacOptions += "-feature"
scalacOptions += "-deprecation"

publishMavenStyle := true

licenses := Seq("Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0"))
homepage := Some(url("http://t-streams.com/"))

resolvers += "Sonatype OSS" at "https://oss.sonatype.org/service/local/staging/deploy/maven2"
resolvers += "Sonatype OSS snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"


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
  "org.scalatest" % "scalatest_2.12" % "3.0.1",
  "org.scalamock" % "scalamock-scalatest-support_2.12" % "3.5.0",
  "io.netty" % "netty-all" % "4.1.7.Final",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.12" % "2.8.6",
  "net.openhft" % "chronicle-queue" % "4.2.6",
  "org.scala-lang" % "scala-reflect" % "2.12.1",
  "org.scala-lang.modules" % "scala-xml_2.12" % "1.0.6",
  "log4j" % "log4j" % "1.2.17",
  "org.apache.curator" % "curator-recipes" % "2.11.0",
  "com.google.guava" % "guava" % "18.0")


//ASSEMBLY STRATEGY
assemblyJarName in assembly := "t-streams-" + tstreamsVersion + ".jar"

assemblyMergeStrategy in assembly := {
  case PathList("org", "slf4j", "impl", xs@_*) => MergeStrategy.discard
  case PathList("org", "hamcrest", xs@_*) => MergeStrategy.discard
  case PathList("io", "netty", xs@_*) => MergeStrategy.first
  case PathList("org", "slf4j", xs@_*) => MergeStrategy.first
  case PathList("org", "scalatest", xs@_*) => MergeStrategy.first
  case PathList("com", "fasterxml", "jackson", "module", xs@_*) => MergeStrategy.first
  case PathList("net", "openhft", xs@_*) => MergeStrategy.first
  case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
}

//TESTS
parallelExecution in ThisBuild := false