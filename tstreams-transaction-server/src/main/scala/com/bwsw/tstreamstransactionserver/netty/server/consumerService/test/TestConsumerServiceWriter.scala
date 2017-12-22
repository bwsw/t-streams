package com.bwsw.tstreamstransactionserver.netty.server.consumerService.test

import com.bwsw.tstreamstransactionserver.netty.server.Notifier
import com.bwsw.tstreamstransactionserver.netty.server.consumerService.{ConsumerServiceWriter, ConsumerTransactionRecord}
import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDbManager
import com.bwsw.tstreamstransactionserver.rpc.ConsumerTransaction

class TestConsumerServiceWriter(rocksMetaServiceDB: KeyValueDbManager,
                                notifier: Notifier[ConsumerTransaction])
  extends ConsumerServiceWriter(rocksMetaServiceDB) {

  override protected def onStateChange: (ConsumerTransactionRecord) => Unit = {
    transaction => {
      notifier.tryCompleteRequests(transaction)
    }
  }
}
