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

package com.bwsw.tstreamstransactionserver.benchmark.utils.writer

import com.bwsw.tstreamstransactionserver.benchmark.Options._
import com.bwsw.tstreamstransactionserver.benchmark.utils.{CsvWriter, TimeMeasure, TransactionCreator}
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates

import scala.concurrent.Await
import scala.concurrent.duration._

class TransactionDataWriter(streamID: Int, partition: Int = 1) extends TransactionCreator with CsvWriter with TimeMeasure {
  def run(txnCount: Int, dataSize: Int, filename: String) {

    val client = clientBuilder
      .build()

    var producerTransaction = createTransaction(streamID, partition, TransactionStates.Opened)
    println("Open a txn: " + time(Await.result(client.putTransactions(Seq(producerTransaction), Seq()),10.seconds)) + " ms")

    val data = createTransactionData(dataSize)

    var globalProgress = 1
    val result = (1 to txnCount).map(x => {
      val localProgress = (x.toDouble / txnCount * 100).round
      if (globalProgress == localProgress) {
        println(localProgress + "%")
        globalProgress += 1
      }

      x -> time(Await.result(client.putTransactionData(producerTransaction.stream, producerTransaction.partition, producerTransaction.transactionID, data, (txnCount - 1) * dataSize), 10.seconds))
    })

    println(s"Write to file $filename")
    writeDataTransactionsAndTime(filename, result)

    producerTransaction = createTransaction(streamID, partition, TransactionStates.Checkpointed, producerTransaction.transactionID)
    println("Close a txn: " + time(Await.result(client.putTransactions(Seq(producerTransaction), Seq()),10.seconds)) + " ms")

    client.shutdown()
  }
}



