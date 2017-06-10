import Publish._
import sbt.Keys._
import sbt._
import sbtassembly.AssemblyPlugin.autoImport._
import sbtassembly.PathList

object Common {

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

  val projectSettings =
    Dependencies.Common ++
    Dependencies.`BW-SW` ++ Seq(
      scalacOptions ++= Seq(
        "-deprecation", "-feature"
      ),

      javacOptions ++= Seq(
        "-Dsun.net.maxDatagramSockets=1000"
      ),

      resolvers ++= Seq("Sonatype OSS" at "https://oss.sonatype.org/service/local/staging/deploy/maven2",
        "Sonatype OSS snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
        "Twitter Repo" at "https://maven.twttr.com",
        "Oracle Maven2 Repo" at "http://download.oracle.com/maven"),

      parallelExecution in ThisBuild := false, //tests property
      fork := true

    ) ++ assemblyStrategySettings ++ publishSettings
}
