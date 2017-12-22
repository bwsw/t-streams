name := "tstreams-transaction-server"

version := "1.5.0-SNAPSHOT"

scalaVersion := "2.12.4"

pomExtra :=
  <scm>
    <url>git@github.com:bwsw/tstreams-transaction-server.git</url>
    <connection>scm:git@github.com:bwsw/tstreams-transaction-server.git</connection>
  </scm>
    <developers>
      <developer>
        <id>bitworks</id>
        <name>Bitworks Software, Ltd.</name>
        <url>http://bitworks.software/</url>
      </developer>
    </developers>

fork in run := true
fork in Test := true
licenses := Seq("Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0"))
homepage := Some(url("http://t-streams.com/"))
pomIncludeRepository := { _ => false }
scalacOptions += "-feature"
scalacOptions += "-deprecation"
parallelExecution in Test := false
organization := "com.bwsw"
publishMavenStyle := true
pomIncludeRepository := { _ => false }

isSnapshot := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

publishArtifact in Test := false
assemblyJarName in assembly := s"${name.value}-${version.value}.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _@_*) =>
    MergeStrategy.discard
  case _ =>
    MergeStrategy.first
}

val sroogeGenOutput = "src/main/thrift/gen"
ScroogeSBT.autoImport.scroogeThriftOutputFolder in Compile := baseDirectory.value / sroogeGenOutput

val protobufGenOutput = "src/main/protobuf/gen"

ScroogeSBT.autoImport.scroogeBuildOptions in Compile := Seq()
unmanagedSourceDirectories in Compile += baseDirectory.value / "src/main/resources"
managedSourceDirectories in Compile += baseDirectory.value / sroogeGenOutput
managedSourceDirectories in Compile += baseDirectory.value / protobufGenOutput
parallelExecution in Test := false

PB.targets in Compile := Seq(
  scalapb.gen(singleLineToString = true) -> baseDirectory.value / protobufGenOutput
)


resolvers ++= Seq(
  "twitter-repo" at "https://maven.twttr.com",
  "Sonatype OSS" at "https://oss.sonatype.org/service/local/staging/deploy/maven2",
  "Sonatype snapshots OSS" at "https://oss.sonatype.org/content/repositories/snapshots"
)

libraryDependencies ++= Seq(
  "commons-io" % "commons-io" % "2.5",
  "com.twitter" %% "scrooge-core" % "17.10.0",
  ("com.twitter" % "libthrift" % "0.5.0-7")
    .exclude("org.slf4j", "slf4j-api"),

  "org.rocksdb" % "rocksdbjni" % "5.7.3",
  "org.scalactic" %% "scalactic" % "3.0.4",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test",

  ("io.netty" % "netty" % "3.10.6.Final")
    .exclude("log4j", "log4j")
    .exclude("org.slf4j", "slf4j-api"),

  ("io.netty" % "netty-all" % "4.1.15.Final")
    .exclude("log4j", "log4j")
    .exclude("org.slf4j", "slf4j-api"),

  "org.json4s" %% "json4s-jackson" % "3.5.1",

  "org.slf4j" % "slf4j-api" % "1.7.25" % "provided",
  "org.slf4j" % "slf4j-log4j12" % "1.7.25" % "provided",

  "org.apache.zookeeper" % "zookeeper" % "3.4.10" pomOnly(),


  ("org.apache.bookkeeper" % "bookkeeper-server" % "4.5.0")
    .exclude("log4j", "log4j")
    .exclude("org.slf4j", "slf4j-api"),

  "commons-validator" % "commons-validator" % "1.6",

  ("org.apache.curator" % "curator-framework" % "2.12.0")
    .exclude("org.slf4j", "slf4j-api"),
  ("org.apache.curator" % "curator-test" % "2.12.0")
    .exclude("log4j", "log4j")
    .exclude("org.slf4j", "slf4j-api"),
  ("org.apache.curator" % "curator-recipes" % "2.12.0")
    .exclude("log4j", "log4j")
    .exclude("org.slf4j", "slf4j-api"),

  "io.zipkin.reporter2" % "zipkin-sender-okhttp3" % "2.1.4"
)

dependencyOverrides += "org.slf4j" % "slf4j-api" % "1.7.25"
dependencyOverrides += "org.slf4j" % "slf4j-log4j12" % "1.7.25"
dependencyOverrides += "io.netty" % "netty" % "3.10.6.Final"

