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

import com.twitter.scrooge.ScroogeSBT
import sbt.Keys._
import sbt._
import sbtassembly.AssemblyPlugin.autoImport._

object TStreamsTransactionServer {

  val sroogeGenOutput = "src/main/thrift/gen"

  val projectSettings = Common.projectSettings ++ Seq(
    mainClass in Compile := Some("com.bwsw.tstreamstransactionserver.ServerLauncher"),

    assemblyJarName in assembly := s"${name.value}-${version.value}.jar",

    ScroogeSBT.autoImport.scroogeThriftOutputFolder in Compile := (managedSourceDirectories in Compile).value.head,
    ScroogeSBT.autoImport.scroogeBuildOptions in Compile := Seq()
  ) ++ Dependencies.TStreamsTransactionServer
}