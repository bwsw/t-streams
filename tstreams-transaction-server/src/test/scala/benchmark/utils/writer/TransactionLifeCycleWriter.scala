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

package benchmark.utils.writer

import benchmark.utils.{CsvWriter, TimeMeasure, TransactionCreator}
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates

import benchmark.Options._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class TransactionLifeCycleWriter(streamID: Int, partition: Int = 1) extends TransactionCreator with CsvWriter with TimeMeasure {
  def run(txnCount: Int, dataSize: Int, filename: String) {
    val client = clientBuilder
      .build()

    val data = createTransactionData(dataSize)

    implicit val context = ExecutionContext.Implicits.global

    var globalProgress = 1
    val result = (1 to txnCount).map(x => {

      val localProgress = (x.toDouble / txnCount * 100).round
      if (globalProgress == localProgress) {
        println(localProgress + "%")
        globalProgress += 1
      }


      val openedProducerTransaction = createTransaction(streamID, partition, TransactionStates.Opened)
      val closedProducerTransaction = createTransaction(streamID, partition, TransactionStates.Checkpointed, openedProducerTransaction.transactionID)
      val t =
        time(Await.result(
          Future.sequence(Seq(
            client.putProducerState(openedProducerTransaction),
            client.putTransactionData(openedProducerTransaction.stream, openedProducerTransaction.partition, openedProducerTransaction.transactionID, data, (txnCount - 1) * dataSize),
            client.putProducerState(closedProducerTransaction))), 10.seconds)
        )
      (x, t)
    })

    println(s"Write to file $filename")
    writeTransactionsLifeCycleAndTime(filename, result)

    client.shutdown()
  }
}
