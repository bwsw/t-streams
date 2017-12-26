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

package com.bwsw.tstreamstransactionserver.netty.server

import java.nio.ByteBuffer

import com.bwsw.tstreamstransactionserver.exception.Throwable.StreamDoesNotExist
import com.bwsw.tstreamstransactionserver.netty.server.consumerService.{ConsumerServiceWriter, ConsumerTransactionRecord}
import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDbBatch
import com.bwsw.tstreamstransactionserver.netty.server.storage.Storage
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataService
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.{ProducerStateMachineCache, ProducerTransactionRecord, ProducerTransactionsCleaner, TransactionMetaServiceWriter}

class RocksWriter(storage: Storage,
                  transactionDataService: TransactionDataService) {

  protected val consumerService =
    new ConsumerServiceWriter(
      storage.getStorageManager
    )

  protected val producerTransactionsCleaner =
    new ProducerTransactionsCleaner(
      storage.getStorageManager
    )

  protected val producerStateMachineCache =
    new ProducerStateMachineCache(storage.getStorageManager)

  protected val transactionMetaServiceWriter =
    new TransactionMetaServiceWriter(
      storage.getStorageManager,
      producerStateMachineCache
    )

  @throws[StreamDoesNotExist]
  final def putTransactionData(streamID: Int,
                               partition: Int,
                               transaction: Long,
                               data: Seq[ByteBuffer],
                               from: Int): Boolean =
    transactionDataService.putTransactionData(
      streamID,
      partition,
      transaction,
      data,
      from
    )

  final def putTransactions(transactions: Seq[ProducerTransactionRecord],
                            batch: KeyValueDbBatch): Unit = {
    transactionMetaServiceWriter.putTransactions(
      transactions,
      batch
    )
  }

  final def putConsumersCheckpoints(consumerTransactions: Seq[ConsumerTransactionRecord],
                                    batch: KeyValueDbBatch): Unit = {
    consumerService.putConsumersCheckpoints(consumerTransactions, batch)
  }

  final def getNewBatch: KeyValueDbBatch =
    storage.newBatch

  final def createAndExecuteTransactionsToDeleteTask(timestamp: Long): Unit =
    producerTransactionsCleaner
      .cleanExpiredProducerTransactions(timestamp)

  def clearProducerTransactionCache(): Unit =
    producerStateMachineCache.clear()
}
