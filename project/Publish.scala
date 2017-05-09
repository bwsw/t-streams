import sbt._
import Keys._

object Publish {
  val publishSettings = Seq(
    publishMavenStyle := true,

    licenses := Seq("Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0")),

    homepage := Some(url("http://t-streams.com/")),

    pomIncludeRepository := { _ => false },

    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (isSnapshot.value)
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases"  at nexus + "service/local/staging/deploy/maven2")
    },

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

    publishArtifact in Test := false
  )
}
