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
