package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.test

import com.bwsw.tstreamstransactionserver.netty.server.Notifier
import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDbManager
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.{ProducerTransactionRecord, ProducerTransactionsCleaner}
import com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction

class TestProducerTransactionsCleaner(rocksDB: KeyValueDbManager,
                                      notifier: Notifier[ProducerTransaction])
  extends ProducerTransactionsCleaner(rocksDB) {
  override protected def onStateChange: (ProducerTransactionRecord) => Unit = {
    transaction => {
      notifier.tryCompleteRequests(transaction)
    }
  }
}
