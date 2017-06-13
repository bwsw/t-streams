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

import sbt.Keys._
import sbt._

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
