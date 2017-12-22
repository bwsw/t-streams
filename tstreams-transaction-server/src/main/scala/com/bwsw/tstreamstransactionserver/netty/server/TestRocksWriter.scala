package com.bwsw.tstreamstransactionserver.netty.server

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
                      consumerTransactionNotifier: Notifier[ConsumerTransaction])
  extends RocksWriter(
    storage,
    transactionDataService) {

  override protected val consumerService: ConsumerServiceWriter =
    new TestConsumerServiceWriter(
      storage.getStorageManager,
      consumerTransactionNotifier
    )

  override protected val producerTransactionsCleaner: ProducerTransactionsCleaner =
    new TestProducerTransactionsCleaner(
      storage.getStorageManager,
      producerTransactionNotifier
    )

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
