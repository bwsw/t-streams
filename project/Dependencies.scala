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

object Dependencies {

  val Common = Seq(
    libraryDependencies ++= Seq(
    "org.slf4j" % "slf4j-api" % "1.7.24",
    "org.slf4j" % "slf4j-log4j12" % "1.7.24",
    "org.apache.curator" % "curator-recipes" % "2.12.0",
    "org.scalatest" %% "scalatest" % "3.0.1" % "test",
    "org.scalamock" %% "scalamock-scalatest-support" % "3.5.0" % "test"
  ))

  val `BW-SW` = Seq(
    libraryDependencies ++= Seq(
      "com.bwsw" % "tstreams-transaction-server_2.12" % "1.5.0-SNAPSHOT"
    ))
}


