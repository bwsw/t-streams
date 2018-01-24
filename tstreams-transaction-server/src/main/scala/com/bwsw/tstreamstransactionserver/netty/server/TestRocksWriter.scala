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

import com.bwsw.tstreamstransactionserver.netty.server.authService.OpenedTransactions
import com.bwsw.tstreamstransactionserver.netty.server.consumerService.ConsumerServiceWriter
import com.bwsw.tstreamstransactionserver.netty.server.consumerService.test.TestConsumerServiceWriter
import com.bwsw.tstreamstransactionserver.netty.server.storage.Storage
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataService
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.test.{TestProducerTransactionsCleaner, TestTransactionMetaServiceWriter}
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.{ProducerTransactionsCleaner, TransactionMetaServiceWriter}
import com.bwsw.tstreamstransactionserver.rpc.{ConsumerTransaction, ProducerTransaction}

class TestRocksWriter(storage: Storage,
                      transactionDataService: TransactionDataService,
                      producerTransactionNotifier: Notifier[ProducerTransaction],
                      consumerTransactionNotifier: Notifier[ConsumerTransaction],
                      openedTransactions: OpenedTransactions)
  extends RocksWriter(
    storage,
    transactionDataService,
    openedTransactions) {

  override protected val consumerService: ConsumerServiceWriter =
    new TestConsumerServiceWriter(
      storage.getStorageManager,
      consumerTransactionNotifier
    )

  override protected val producerTransactionsCleaner: ProducerTransactionsCleaner =
    new TestProducerTransactionsCleaner(
      storage.getStorageManager,
      producerTransactionNotifier,
      openedTransactions)

  override protected val transactionMetaServiceWriter: TransactionMetaServiceWriter =
    new TestTransactionMetaServiceWriter(
      storage.getStorageManager,
      producerStateMachineCache,
      producerTransactionNotifier
    )

  override def clearProducerTransactionCache(): Unit = {
    producerTransactionNotifier.broadcastNotifications()
    consumerTransactionNotifier.broadcastNotifications()
    super.clearProducerTransactionCache()
  }
}
