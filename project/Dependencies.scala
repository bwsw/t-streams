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
      "org.slf4j" % "slf4j-api" % "1.7.25",
      "org.slf4j" % "slf4j-log4j12" % "1.7.25",
      "org.apache.curator" % "curator-recipes" % "2.12.0",
      "org.scalatest" %% "scalatest" % "3.0.4" % "test",
      "org.scalamock" %% "scalamock-scalatest-support" % "3.5.0" % "test",
      "org.rogach" %% "scallop" % "3.1.1" % "test",
      "org.apache.commons" % "commons-math3" % "3.6.1" % "test",
      "org.mockito" % "mockito-core" % "2.13.0" % "test"
    ))

  val TStreamsTransactionServer = Seq(
    libraryDependencies ++= Seq(
      "commons-io" % "commons-io" % "2.5",
      "com.twitter" %% "scrooge-core" % "17.10.0",
      "com.twitter" % "libthrift" % "0.5.0-7",
      "org.rocksdb" % "rocksdbjni" % "5.7.5",
      "org.scalactic" %% "scalactic" % "3.0.4",
      "io.netty" % "netty" % "3.10.6.Final",
      "io.netty" % "netty-all" % "4.1.19.Final",
      "org.json4s" %% "json4s-jackson" % "3.5.3",
      "org.apache.zookeeper" % "zookeeper" % "3.4.11" pomOnly(),
      "org.apache.bookkeeper" % "bookkeeper-server" % "4.5.1",
      "commons-validator" % "commons-validator" % "1.6",
      "org.apache.curator" % "curator-framework" % "2.12.0",
      "org.apache.curator" % "curator-test" % "2.12.0",
      "io.zipkin.reporter2" % "zipkin-sender-okhttp3" % "2.2.2"
    )
  )
}


