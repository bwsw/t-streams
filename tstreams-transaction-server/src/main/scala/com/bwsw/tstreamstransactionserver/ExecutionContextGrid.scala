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

package com.bwsw.tstreamstransactionserver

import java.util.concurrent.ThreadPoolExecutor.DiscardPolicy
import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import com.google.common.util.concurrent.ThreadFactoryBuilder

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

/** Context is a wrapper for java executors
  *
  * @constructor creates a context with a number of executors services.
  * @param nContexts a number of executors services
  * @param f         an executor service.
  *
  */
class ExecutionContextGrid(nContexts: Int,
                           f: => java.util.concurrent.ExecutorService) {
  require(nContexts > 0)

  private val contexts =
    Array.fill(nContexts)(newExecutionContext)

  def getContext(value: Long): ExecutionContextExecutorService =
    contexts((value % nContexts).toInt)

  def stopAccessNewTasks(): Unit =
    contexts.foreach(_.shutdown())

  def awaitAllCurrentTasksAreCompleted(): Unit =
    contexts.foreach(_.awaitTermination(
      ExecutionContextGrid.TASK_TERMINATION_MAX_WAIT_MS,
      TimeUnit.MILLISECONDS)
    )

  private def newExecutionContext =
    ExecutionContext.fromExecutorService(f)
}

class SinglePoolExecutionContextGrid(f: => java.util.concurrent.ExecutorService)
  extends ExecutionContextGrid(1, f) {
  def getContext: ExecutionContextExecutorService = getContext(0)
}

object ExecutionContextGrid {
  /** The time to wait all tasks completed by thread pool */
  private val TASK_TERMINATION_MAX_WAIT_MS = 10000

  /** Creates an 1 single-threaded context with name */
  def apply(nameFormat: String) =
    new SinglePoolExecutionContextGrid(
      new ThreadPoolExecutor(
        1,
        1,
        0L,
        TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue(),
        new ThreadFactoryBuilder().setNameFormat(nameFormat).build(),
        new DiscardPolicy()
      )
    )

  /** Creates FixedThreadPool with defined threadNumber */
  def apply(threadNumber: Int, nameFormat: String) =
    new SinglePoolExecutionContextGrid(
      new ThreadPoolExecutor(
        threadNumber,
        threadNumber,
        0L,
        TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue(),
        new ThreadFactoryBuilder().setNameFormat(nameFormat).build(),
        new DiscardPolicy()
      )

    )
}

