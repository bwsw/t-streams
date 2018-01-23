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

package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService

import java.util.concurrent.{Executors, TimeUnit}

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService.hierarchy.LongNodeCache
import com.google.common.util.concurrent.ThreadFactoryBuilder

class BookkeeperSlaveBundle(bookkeeperSlave: BookkeeperSlave,
                            lastClosedLedgerHandlers: Array[LongNodeCache],
                            timeBetweenCreationOfLedgersMs: Int) {

  private lazy val bookKeeperExecutor =
    Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder().setNameFormat("bookkeeper-slave-%d").build()
    )

  private lazy val futureTask =
    bookKeeperExecutor.scheduleWithFixedDelay(
      bookkeeperSlave,
      0L,
      timeBetweenCreationOfLedgersMs,
      TimeUnit.NANOSECONDS
    )

  def start(): Unit = {
    futureTask
  }


  def stop(): Unit = {
    futureTask.cancel(true)
    bookKeeperExecutor.shutdown()
    lastClosedLedgerHandlers.foreach(_.stopMonitor())
    scala.util.Try {
      bookKeeperExecutor.awaitTermination(
        0L,
        TimeUnit.NANOSECONDS
      )
    }
  }

}
