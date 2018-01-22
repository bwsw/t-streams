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

package benchmark.utils

import com.bwsw.tstreamstransactionserver.rpc.{ProducerTransaction, TransactionStates}

import scala.collection.immutable.IndexedSeq

trait TransactionCreator {
  def createProducerTransactions(streamID: Int, partition: Int, _type: TransactionStates, count: Int): Seq[ProducerTransaction] =
    Seq.fill(count)(createTransaction(streamID, partition, _type))

  def createTransaction(streamID: Int, _partition: Int, _type: TransactionStates): ProducerTransaction = {
    new ProducerTransaction {
      override val transactionID: Long = System.nanoTime()

      override val state: TransactionStates = _type

      override val stream: Int = streamID

      override val ttl: Long = System.currentTimeMillis()

      override val quantity: Int = -1

      override val partition: Int = _partition
    }
  }

  def createTransaction(streamID: Int, _partition: Int, _type: TransactionStates, id: Long): ProducerTransaction = {
    new ProducerTransaction {
      override val transactionID: Long = id

      override val state: TransactionStates = _type

      override val stream: Int = streamID

      override val ttl: Long = System.currentTimeMillis()

      override val quantity: Int = -1

      override val partition: Int = _partition
    }
  }

  def createTransactionData(count: Int): Seq[Array[Byte]] =
    Seq.fill(count)(new Array[Byte](4))
}
