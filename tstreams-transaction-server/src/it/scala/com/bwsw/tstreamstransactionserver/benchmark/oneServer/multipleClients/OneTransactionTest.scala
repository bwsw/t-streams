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

package com.bwsw.tstreamstransactionserver.benchmark.oneServer.multipleClients

import com.bwsw.tstreamstransactionserver.benchmark.utils.Launcher
import com.bwsw.tstreamstransactionserver.benchmark.utils.writer.TransactionDataWriter

import scala.collection.mutable._
import scala.util.Random

object OneTransactionTest extends Launcher {
  override val streamName = "stream"
  override val clients = 2
  private val txnCount = 100000
  private val dataSize = 1000
  private val clientThreads = ArrayBuffer[Thread]()

  def main(args: Array[String]) {
    launch()
    System.exit(0)
  }

  override def launchClients(streamID: Int): Unit = {
    (1 to clients).foreach(x => {
      val thread = new Thread(() => {
        val filename = s"${Random.nextInt(100)}TransactionDataWriterTo${x}PartitionOSMC.csv"
        new TransactionDataWriter(streamID, x).run(txnCount, dataSize, filename)
      })
      clientThreads.+=(thread)
    })
    clientThreads.foreach(_.start())
    clientThreads.foreach(_.join())
  }
}