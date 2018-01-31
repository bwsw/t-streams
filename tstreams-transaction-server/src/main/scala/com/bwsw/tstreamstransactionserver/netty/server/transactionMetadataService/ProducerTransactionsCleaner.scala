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

import com.bwsw.tstreamstransactionserver.netty.server.authService.OpenedTransactions
import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDbManager
import com.bwsw.tstreamstransactionserver.netty.server.storage.Storage
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates.Invalid
import org.slf4j.{Logger, LoggerFactory}

class ProducerTransactionsCleaner(rocksDB: KeyValueDbManager, openedTransactions: OpenedTransactions) {
  private val logger: Logger =
    LoggerFactory.getLogger(this.getClass)

  private val openedProducerTransactionsDatabase =
    rocksDB.getDatabase(Storage.TRANSACTION_OPEN_STORE)

  def cleanExpiredProducerTransactions(timestampToDeleteTransactions: Long): Unit = {
    scala.util.Try {
      openedTransactions.tokenCache.removeExpired(timestampToDeleteTransactions)

      def isExpired(transaction: ProducerTransactionRecord): Boolean = {
        math.abs(transaction.timestamp + transaction.ttl) <= timestampToDeleteTransactions ||
          !openedTransactions.isValid(transaction.transactionID)
      }


      if (logger.isDebugEnabled) {
        logger.debug(s"Cleaner[time: $timestampToDeleteTransactions] of expired transactions is running.")
      }
      val batch = rocksDB.newBatch

      val iterator = openedProducerTransactionsDatabase.iterator
      iterator.seekToFirst()

      while (iterator.isValid) {
        val producerTransactionValue = ProducerTransactionValue.fromByteArray(iterator.value())
        val key = iterator.key()
        val producerTransactionKey = ProducerTransactionKey.fromByteArray(key)
        val producerTransactionRecord = ProducerTransactionRecord(
          producerTransactionKey,
          producerTransactionValue)

        if (isExpired(producerTransactionRecord)) {
          if (logger.isDebugEnabled) {
            logger.debug(s"Cleaning $producerTransactionRecord as it's expired.")
          }

          openedTransactions.remove(producerTransactionRecord.transactionID)

          val canceledTransactionRecordDueExpiration =
            transitProducerTransactionToInvalidState(producerTransactionRecord)

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
