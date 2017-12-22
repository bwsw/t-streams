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
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler._
import com.bwsw.tstreamstransactionserver.rpc._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer


class TransactionMetaServiceWriter(rocksDB: KeyValueDbManager,
                                   producerStateMachineCache: ProducerStateMachineCache) {

  import producerStateMachineCache._

  private val logger: Logger =
    LoggerFactory.getLogger(this.getClass)

  def putTransactions(transactions: Seq[ProducerTransactionRecord],
                      batch: KeyValueDbBatch): Unit = {

    val producerTransactions =
      selectInOrderProducerTransactions(transactions, batch)

    val groupedProducerTransactions =
      groupProducerTransactionsByStreamPartitionTransactionID(producerTransactions)

    groupedProducerTransactions.foreach { case (key, txns) =>
      //retrieving an opened transaction from opened transaction database if it exist
      val openedTransactionOpt = producerStateMachineCache.getProducerTransaction(key)

      val sortedTransactions = txns.sorted
      val transactionsToProcess = openedTransactionOpt
        .map(data =>
          ProducerTransactionRecord(key, data) +: sortedTransactions
        )
        .getOrElse(sortedTransactions)

      val finalStateOpt = ProducerTransactionStateMachine
        .transiteTransactionsToFinalState(
          transactionsToProcess,
          onStateChange
        )

      finalStateOpt
        .filter(ProducerTransactionStateMachine.checkFinalStateBeStoredInDB)
        .foreach { finalState =>
          val transaction = finalState.producerTransactionRecord
          putTransactionToAllAndOpenedTables(transaction, batch)
        }
    }
  }

  private final def selectInOrderProducerTransactions(transactions: Seq[ProducerTransactionRecord],
                                                      batch: KeyValueDbBatch) = {
    val producerTransactions = ArrayBuffer[ProducerTransactionRecord]()
    transactions foreach { txn =>
      val key = KeyStreamPartition(txn.stream, txn.partition)
      if (txn.state != TransactionStates.Opened) {
        producerTransactions += txn
      } else if (!isThatTransactionOutOfOrder(key, txn.transactionID)) {
        // updating RAM table, and last opened transaction database.

        updateLastOpenedTransactionID(
          key,
          txn.transactionID
        )

        putLastOpenedTransactionID(
          key,
          txn.transactionID,
          batch
        )

        if (logger.isDebugEnabled)
          logger.debug(
            s"On stream:${key.stream} partition:${key.partition} " +
              s"last opened transaction is ${txn.transactionID} now."
          )
        producerTransactions += txn
      }
    }
    producerTransactions
  }

  private final def groupProducerTransactionsByStreamPartitionTransactionID(producerTransactions: Seq[ProducerTransactionRecord]) =
    producerTransactions.groupBy(txn => txn.key)

  private def putTransactionToAllAndOpenedTables(producerTransactionRecord: ProducerTransactionRecord,
                                                 batch: KeyValueDbBatch) = {

    if (producerTransactionRecord.state == TransactionStates.Checkpointed) {
      updateLastCheckpointedTransactionAndPutToDatabase(
        stateHandler.KeyStreamPartition(
          producerTransactionRecord.stream,
          producerTransactionRecord.partition
        ),
        producerTransactionRecord,
        batch
      )
    }

    val binaryTxn = producerTransactionRecord.producerTransaction.toByteArray
    val binaryKey = producerTransactionRecord.key.toByteArray

    producerStateMachineCache.updateProducerTransaction(
      producerTransactionRecord.key,
      producerTransactionRecord.producerTransaction
    )

    if (producerTransactionRecord.state == TransactionStates.Opened) {
      batch.put(Storage.TRANSACTION_OPEN_STORE, binaryKey, binaryTxn)
    }
    else {
      batch.remove(Storage.TRANSACTION_OPEN_STORE, binaryKey)
    }
    batch.put(Storage.TRANSACTION_ALL_STORE, binaryKey, binaryTxn)

    if (logger.isDebugEnabled)
      logger.debug(s"Producer transaction on stream: ${producerTransactionRecord.stream}" +
        s"partition ${producerTransactionRecord.partition}, transactionId ${producerTransactionRecord.transactionID} " +
        s"with state ${producerTransactionRecord.state} is ready for commit"
      )
  }

  private final def updateLastCheckpointedTransactionAndPutToDatabase(key: stateHandler.KeyStreamPartition,
                                                                      producerTransactionWithNewState: ProducerTransactionRecord,
                                                                      batch: KeyValueDbBatch): Unit = {
    updateLastCheckpointedTransactionID(
      key,
      producerTransactionWithNewState.transactionID
    )

    putLastCheckpointedTransactionID(
      key,
      producerTransactionWithNewState.transactionID,
      batch
    )
    if (logger.isDebugEnabled())
      logger.debug(
        s"On stream:${key.stream} partition:${key.partition} " +
          s"last checkpointed transaction is ${producerTransactionWithNewState.transactionID} now."
      )
  }

  protected def onStateChange: ProducerTransactionRecord => Unit =
    _ => {}
}
