/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
    case PathList("org", "apache", "commons", "collections", xs@_*) => MergeStrategy.first
    case PathList("org", "apache", "commons", "beanutils", xs@_*) => MergeStrategy.first
    case x if x.endsWith("io.netty.versions.properties") => MergeStrategy.first
    case "log4j.properties" => MergeStrategy.concat
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  })

  val projectSettings = Seq(
    version := "3.0.6-SNAPSHOT",
    isSnapshot := true,
    scalaVersion := "2.12.4",
    organization := "com.bwsw",
    organizationName := "Bitworks Software, Ltd.",
    organizationHomepage := Some(url("https://bitworks.software/")),

    scalacOptions ++= Seq(
      "-deprecation", "-feature"
    ),

    javacOptions ++= Seq(
      "-Dsun.net.maxDatagramSockets=1000"
    ),

    testOptions += Tests.Argument(
      TestFrameworks.ScalaTest,
      "-oFD", // to show full stack traces and durations
      "-W", "120", "60" // to notify when some test is running longer than a specified amount of time
    ),

    resolvers ++= Seq(
      "Sonatype OSS" at "https://oss.sonatype.org/service/local/staging/deploy/maven2",
      "Sonatype OSS snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
      "Twitter Repo" at "https://maven.twttr.com",
      "Oracle Maven2 Repo" at "http://download.oracle.com/maven"),

    parallelExecution in ThisBuild := false, //tests property
    fork := true,
    fork in run := true,
    fork in Test := true,

    connectInput in run := true,
    outputStrategy := Some(StdoutOutput) // to suppress logging prefixes in sbt runMain
  ) ++
    assemblyStrategySettings ++
    publishSettings ++
    Defaults.itSettings ++
    Dependencies.Test
}
