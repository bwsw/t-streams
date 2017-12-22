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
package com.bwsw.tstreamstransactionserver.configProperties

import com.bwsw.tstreamstransactionserver.ExecutionContextGrid

import scala.concurrent.ExecutionContextExecutorService

class ServerExecutionContextGrids(rocksWriteNThreads: Int, rocksReadNThreads: Int) {
//  private lazy val commitLogExecutionContextGrid =
//    ExecutionContextGrid("CommitLogExecutionContextGrid-%d")
  private lazy val serverWriteExecutionContextGrid =
    ExecutionContextGrid(rocksWriteNThreads, "ServerWriteExecutionContextGrid-%d")
  private lazy val serverReadExecutionContextGrid =
    ExecutionContextGrid(rocksReadNThreads, "ServerReadExecutionContextGrid-%d")

  private lazy val contextGrids = Seq(
//    commitLogExecutionContextGrid,
    serverReadExecutionContextGrid,
    serverWriteExecutionContextGrid
  )

//  val commitLogContext: ExecutionContextExecutorService = commitLogExecutionContextGrid.getContext
  val serverWriteContext: ExecutionContextExecutorService = serverWriteExecutionContextGrid.getContext
  val serverReadContext: ExecutionContextExecutorService = serverReadExecutionContextGrid.getContext

  def stopAccessNewTasksAndAwaitAllCurrentTasksAreCompleted(): Unit = {
    contextGrids foreach (context => context.stopAccessNewTasks())
    contextGrids foreach (context => context.awaitAllCurrentTasksAreCompleted())
  }
}
