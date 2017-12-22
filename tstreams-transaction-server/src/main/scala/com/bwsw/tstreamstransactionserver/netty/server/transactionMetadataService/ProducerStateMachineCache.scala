package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService


import com.bwsw.tstreamstransactionserver.netty.server.db.{KeyValueDbBatch, KeyValueDbManager}
import com.bwsw.tstreamstransactionserver.netty.server.storage.Storage
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler.{KeyStreamPartition, LastTransaction, TransactionId}

import scala.collection.mutable

final class ProducerStateMachineCache(rocksDB: KeyValueDbManager) {
  private val producerTransactionsWithOpenedStateDatabase =
    rocksDB.getDatabase(Storage.TRANSACTION_OPEN_STORE)

  private val transactionsRamTable =
    mutable.Map.empty[ProducerTransactionKey, ProducerTransactionValue]

  private val lastTransactionStreamPartitionRamTable =
    mutable.Map.empty[KeyStreamPartition, LastTransaction]


  def getProducerTransaction(key: ProducerTransactionKey): Option[ProducerTransactionValue] = {
    transactionsRamTable.get(key)
      .orElse {
        val keyFound = key.toByteArray
        Option(
          producerTransactionsWithOpenedStateDatabase.get(keyFound)
        ).map { data =>
          val producerTransactionValue =
            ProducerTransactionValue.fromByteArray(data)

          transactionsRamTable.put(
            key,
            producerTransactionValue
          )

          producerTransactionValue
        }
      }
  }

  def updateProducerTransaction(key: ProducerTransactionKey,
                                value: ProducerTransactionValue): Unit = {
    transactionsRamTable.put(key, value)
  }

  def removeProducerTransaction(key: ProducerTransactionKey): Unit = {
    transactionsRamTable -= key
  }


  //  private val comparator = com.bwsw.tstreamstransactionserver.`implicit`.Implicits.ByteArray
  //  final def deleteLastOpenedAndCheckpointedTransactions(streamID: Int, batch: KeyValueDatabaseBatch) {
  //    val from = KeyStreamPartition(streamID, Int.MinValue).toByteArray
  //    val to = KeyStreamPartition(streamID, Int.MaxValue).toByteArray
  //
  //    val lastTransactionDatabaseIterator = lastTransactionDatabase.iterator
  //    lastTransactionDatabaseIterator.seek(from)
  //    while (
  //      lastTransactionDatabaseIterator.isValid &&
  //        comparator.compare(lastTransactionDatabaseIterator.key(), to) <= 0
  //    ) {
  //      batch.remove(RocksStorage.LAST_OPENED_TRANSACTION_STORAGE, lastTransactionDatabaseIterator.key())
  //      lastTransactionDatabaseIterator.next()
  //    }
  //    lastTransactionDatabaseIterator.close()
  //
  //    val lastCheckpointedTransactionDatabaseIterator = lastCheckpointedTransactionDatabase.iterator
  //    lastCheckpointedTransactionDatabaseIterator.seek(from)
  //    while (
  //      lastCheckpointedTransactionDatabaseIterator.isValid &&
  //      comparator.compare(lastCheckpointedTransactionDatabaseIterator.key(), to) <= 0)
  //    {
  //      batch.remove(RocksStorage.LAST_CHECKPOINTED_TRANSACTION_STORAGE, lastCheckpointedTransactionDatabaseIterator.key())
  //      lastCheckpointedTransactionDatabaseIterator.next()
  //    }
  //    lastCheckpointedTransactionDatabaseIterator.close()
  //  }

  def putLastOpenedTransactionID(key: KeyStreamPartition,
                                 transactionId: Long,
                                 batch: KeyValueDbBatch): Boolean = {
    putLastTransaction(
      key,
      transactionId,
      batch,
      Storage.LAST_OPENED_TRANSACTION_STORAGE
    )
  }

  def putLastCheckpointedTransactionID(key: KeyStreamPartition,
                                       transactionId: Long,
                                       batch: KeyValueDbBatch): Boolean = {
    putLastTransaction(
      key,
      transactionId,
      batch,
      Storage.LAST_CHECKPOINTED_TRANSACTION_STORAGE
    )
  }

  private def putLastTransaction(key: KeyStreamPartition,
                                 transactionId: Long,
                                 batch: KeyValueDbBatch,
                                 databaseIndex: Int): Boolean = {
    val updatedTransactionID =
      new TransactionId(transactionId)
    val binaryKey =
      key.toByteArray
    val binaryTransactionID =
      updatedTransactionID.toByteArray

    batch.put(
      databaseIndex,
      binaryKey,
      binaryTransactionID
    )
  }

  def updateLastOpenedTransactionID(key: KeyStreamPartition,
                                    transaction: Long): Unit = {
    val lastOpenedAndCheckpointedTransaction =
      lastTransactionStreamPartitionRamTable.get(key)

    val checkpointedTransactionID =
      lastOpenedAndCheckpointedTransaction
        .map(_.checkpointed)
        .getOrElse(Option.empty[TransactionId])

    lastTransactionStreamPartitionRamTable.put(
      key,
      LastTransaction(
        TransactionId(transaction),
        checkpointedTransactionID
      )
    )
  }

  def updateLastCheckpointedTransactionID(key: KeyStreamPartition,
                                          transaction: Long): Unit = {
    val lastOpenedAndCheckpointedTransaction =
      lastTransactionStreamPartitionRamTable.get(key)

    lastOpenedAndCheckpointedTransaction
      .map(_.opened)
      .foreach { openedTransactionID =>
        lastTransactionStreamPartitionRamTable.put(
          key,
          LastTransaction(
            openedTransactionID,
            Some(TransactionId(transaction))
          )
        )
      }
  }


  def isThatTransactionOutOfOrder(key: KeyStreamPartition,
                                  thatTransactionId: Long): Boolean = {
    val lastTransactionOpt =
      lastTransactionStreamPartitionRamTable.get(key)

    val result = lastTransactionOpt
      .map(_.opened.id)
      .exists(_ > thatTransactionId)

    result
  }

  def clear(): Unit = {
    lastTransactionStreamPartitionRamTable.clear()
    transactionsRamTable.clear()
  }
}
