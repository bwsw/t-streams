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

package benchmark.oneServer.oneClient

import benchmark.utils.Launcher
import benchmark.utils.writer.TransactionMetadataWriter

import scala.util.Random

object MultipleTransactionsTest extends Launcher {
  override val streamName = "stream"
  override val clients = 1
  private val txnCount = 1000000

  def main(args: Array[String]) {
    launch()
    System.exit(0)
  }

  override def launchClients(streamID: Int): Unit = {
    val filename = s"${Random.nextInt(100)}_${txnCount}TransactionMetadataWriterOSOC.csv"
    new TransactionMetadataWriter(streamID).run(txnCount, filename)
  }
}
