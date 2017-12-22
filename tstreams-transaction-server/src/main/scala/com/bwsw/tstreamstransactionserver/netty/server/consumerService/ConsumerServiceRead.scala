package com.bwsw.tstreamstransactionserver.netty.server.consumerService

import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDbManager
import com.bwsw.tstreamstransactionserver.netty.server.storage.Storage
import org.slf4j.LoggerFactory

class ConsumerServiceRead(rocksMetaServiceDB: KeyValueDbManager) {
  private val logger =
    LoggerFactory.getLogger(this.getClass)
  private val consumerDatabase =
    rocksMetaServiceDB.getDatabase(Storage.CONSUMER_STORE)

  def getConsumerState(name: String,
                       streamID: Int,
                       partition: Int): Long = {
    val consumerTransactionKey =
      ConsumerTransactionKey(name, streamID, partition).toByteArray
    val consumerTransactionValue =
      Option(consumerDatabase.get(consumerTransactionKey))

    consumerTransactionValue.map(bytes =>
      ConsumerTransactionValue.fromByteArray(bytes).transactionId
    ).getOrElse {
      if (logger.isDebugEnabled())
        logger.debug(s"There is no checkpointed consumer transaction " +
          s"on stream $name, " +
          s"partition $partition " +
          s"with name: $name. " +
          s"Returning -1L")
      -1L
    }
  }
}
