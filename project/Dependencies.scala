import sbt.Keys._
import sbt._

object Dependencies {

  val Common = Seq(
    libraryDependencies ++= Seq(
    "org.slf4j" % "slf4j-api" % "1.7.24",
    "org.slf4j" % "slf4j-log4j12" % "1.7.24",
    "org.apache.curator" % "curator-recipes" % "2.12.0",
    "com.trueaccord.scalapb" %% "compilerplugin" % "0.6.0-pre3",
    "org.scalatest" %% "scalatest" % "3.0.1" % "test",
    "org.scalamock" %% "scalamock-scalatest-support" % "3.5.0" % "test"
  ))

  val `BW-SW` = Seq(
    libraryDependencies ++= Seq(
      "com.bwsw" % "tstreams-transaction-server_2.12" % "1.3.7.6-SNAPSHOT"
    ))
}


