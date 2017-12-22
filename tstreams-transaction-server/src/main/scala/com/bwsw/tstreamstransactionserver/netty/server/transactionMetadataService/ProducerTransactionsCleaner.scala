package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService

import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDbManager
import com.bwsw.tstreamstransactionserver.netty.server.storage.Storage
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates.Invalid
import org.slf4j.{Logger, LoggerFactory}

class ProducerTransactionsCleaner(rocksDB: KeyValueDbManager) {
  private val logger: Logger =
    LoggerFactory.getLogger(this.getClass)

  private val openedProducerTransactionsDatabase =
    rocksDB.getDatabase(Storage.TRANSACTION_OPEN_STORE)

  def cleanExpiredProducerTransactions(timestampToDeleteTransactions: Long): Unit = {
    scala.util.Try {
      def isExpired(producerTransactionWithoutKey: ProducerTransactionValue): Boolean = {
        scala.math.abs(
          producerTransactionWithoutKey.timestamp +
            producerTransactionWithoutKey.ttl
        ) <= timestampToDeleteTransactions
      }


      if (logger.isDebugEnabled)
        logger.debug(s"Cleaner[time: $timestampToDeleteTransactions] of expired transactions is running.")
      val batch = rocksDB.newBatch

      val iterator = openedProducerTransactionsDatabase.iterator
      iterator.seekToFirst()

      while (iterator.isValid) {
        val producerTransactionValue =
          ProducerTransactionValue.fromByteArray(iterator.value())

        if (isExpired(producerTransactionValue)) {
          if (logger.isDebugEnabled)
            logger.debug(s"Cleaning $producerTransactionValue as it's expired.")

          val key =
            iterator.key()

          val producerTransactionKey =
            ProducerTransactionKey.fromByteArray(key)

          val canceledTransactionRecordDueExpiration =
            transitProducerTransactionToInvalidState(
              ProducerTransactionRecord(
                producerTransactionKey,
                producerTransactionValue
              )
            )

          onStateChange(
            canceledTransactionRecordDueExpiration
          )

          batch.put(
            Storage.TRANSACTION_ALL_STORE,
            key,
            canceledTransactionRecordDueExpiration
              .producerTransaction.toByteArray
          )

          batch.remove(
            Storage.TRANSACTION_OPEN_STORE,
            key
          )
        }
        iterator.next()
      }
      iterator.close()
      batch.write()
    }
  }

  protected def onStateChange: ProducerTransactionRecord => Unit =
    _ => {}

  private def transitProducerTransactionToInvalidState(producerTransactionRecord: ProducerTransactionRecord): ProducerTransactionRecord = {
    val txn = producerTransactionRecord
    ProducerTransactionRecord(
      ProducerTransactionKey(txn.stream, txn.partition, txn.transactionID),
      ProducerTransactionValue(Invalid, 0, 0L, txn.timestamp)
    )
  }
}
