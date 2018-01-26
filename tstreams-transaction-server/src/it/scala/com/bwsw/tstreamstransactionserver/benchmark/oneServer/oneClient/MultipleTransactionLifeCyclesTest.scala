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

package com.bwsw.tstreamstransactionserver.benchmark.oneServer.oneClient

import com.bwsw.tstreamstransactionserver.benchmark.utils.Launcher
import com.bwsw.tstreamstransactionserver.benchmark.utils.writer.TransactionLifeCycleWriter

import scala.util.Random

object MultipleTransactionLifeCyclesTest extends Launcher {
  override val streamName = "stream"
  override val clients = 1
  private val txnCount = 1000000
  private val dataSize = 1

  def main(args: Array[String]) {
    launch()
    System.exit(0)
  }

  override def launchClients(streamID: Int): Unit = {
    val filename = s"${Random.nextInt(100)}_${txnCount}TransactionLifeCycleWriterOSOC.csv"
    new TransactionLifeCycleWriter(streamID).run(txnCount, dataSize, filename)
  }
}