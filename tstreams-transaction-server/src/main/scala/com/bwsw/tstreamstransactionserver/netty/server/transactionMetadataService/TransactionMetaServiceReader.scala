package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService

import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDbManager
import com.bwsw.tstreamstransactionserver.netty.server.storage.Storage
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler.LastTransactionReader
import com.bwsw.tstreamstransactionserver.rpc.{ScanTransactionsInfo, TransactionInfo, TransactionStates}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

class TransactionMetaServiceReader(rocksDB: KeyValueDbManager) {
  private val logger: Logger =
    LoggerFactory.getLogger(this.getClass)

  private val producerTransactionsDatabase =
    rocksDB.getDatabase(Storage.TRANSACTION_ALL_STORE)

  private val lastTransactionReader =
    new LastTransactionReader(rocksDB)
  private val comparator = com.bwsw.tstreamstransactionserver.`implicit`.Implicits.ByteArray

  final def getTransaction(streamID: Int, partition: Int, transaction: Long): com.bwsw.tstreamstransactionserver.rpc.TransactionInfo = {
    val lastTransaction = lastTransactionReader.getLastTransaction(streamID, partition)
    if (lastTransaction.isEmpty || transaction > lastTransaction.get.opened.id) {
      TransactionInfo(exists = false, None)
    } else {
      val searchKey = new ProducerTransactionKey(streamID, partition, transaction).toByteArray

      Option(producerTransactionsDatabase.get(searchKey)).map(searchData =>
        new ProducerTransactionRecord(
          ProducerTransactionKey.fromByteArray(searchKey),
          ProducerTransactionValue.fromByteArray(searchData))
      ) match {
        case None =>
          TransactionInfo(exists = true, None)
        case Some(producerTransactionRecord) =>
          TransactionInfo(exists = true, Some(producerTransactionRecord))
      }
    }
  }

  def scanTransactions(streamID: Int,
                       partition: Int,
                       from: Long,
                       to: Long,
                       count: Int,
                       states: collection.Set[TransactionStates]): com.bwsw.tstreamstransactionserver.rpc.ScanTransactionsInfo = {
    val (lastOpenedTransactionID, toTransactionID) =
      lastTransactionReader.getLastTransaction(streamID, partition) match {
        case Some(lastTransaction) => lastTransaction.opened.id match {
          case lt if lt < from => (lt, from - 1L)
          case lt if from <= lt && lt < to => (lt, lt)
          case lt if lt >= to => (lt, to)
        }
        case None => (-1L, from - 1L)
      }

    if (logger.isDebugEnabled)
      logger.debug(s"Trying to retrieve transactions " +
        s"on stream $streamID, " +
        s"partition: $partition " +
        s"in range [$from, $to]." +
        s"Actually as lt ${if (lastOpenedTransactionID == -1) "doesn't exist" else s"is $lastOpenedTransactionID"} the range is [$from, $toTransactionID].")

    if (toTransactionID < from || count == 0) ScanTransactionsInfo(lastOpenedTransactionID, Seq())
    else {
      val iterator = producerTransactionsDatabase.iterator

      val lastTransactionID = new ProducerTransactionKey(streamID, partition, toTransactionID).toByteArray

      def moveCursorToKey: Option[ProducerTransactionRecord] = {
        val keyFrom = new ProducerTransactionKey(streamID, partition, from)

        iterator.seek(keyFrom.toByteArray)
        val startKey = if (iterator.isValid && comparator.compare(iterator.key(), lastTransactionID) <= 0) {
          Some(
            new ProducerTransactionRecord(
              ProducerTransactionKey.fromByteArray(iterator.key()),
              ProducerTransactionValue.fromByteArray(iterator.value())
            )
          )
        } else None

        iterator.next()

        startKey
      }

      moveCursorToKey match {
        case None =>
          iterator.close()
          ScanTransactionsInfo(lastOpenedTransactionID, Seq())

        case Some(producerTransactionKey) =>
          val producerTransactions = ArrayBuffer[ProducerTransactionRecord](producerTransactionKey)

          var txnState: TransactionStates = producerTransactionKey.state
          while (
            iterator.isValid &&
              producerTransactions.length < count &&
              !states.contains(txnState) &&
              (comparator.compare(iterator.key(), lastTransactionID) <= 0)
          ) {
            val producerTransaction =
              ProducerTransactionRecord(
                ProducerTransactionKey.fromByteArray(iterator.key()),
                ProducerTransactionValue.fromByteArray(iterator.value())
              )
            txnState = producerTransaction.state
            producerTransactions += producerTransaction
            iterator.next()
          }

          iterator.close()

          val result = if (states.contains(txnState))
            producerTransactions.init
          else
            producerTransactions

          ScanTransactionsInfo(lastOpenedTransactionID, result)
      }
    }
  }


}
