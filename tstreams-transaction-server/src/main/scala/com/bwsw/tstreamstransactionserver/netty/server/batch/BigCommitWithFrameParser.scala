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

package com.bwsw.tstreamstransactionserver.netty.server.batch

import com.bwsw.tstreamstransactionserver.netty.server.authService.OpenedTransactions
import com.bwsw.tstreamstransactionserver.netty.server.consumerService.{ConsumerTransactionKey, ConsumerTransactionRecord}
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.ProducerTransactionRecord
import com.bwsw.tstreamstransactionserver.rpc.{Transaction, TransactionStates}

import scala.collection.mutable

class BigCommitWithFrameParser(bigCommit: BigCommit, openedTransactionsCache: OpenedTransactions) {

  private val producerRecords =
    mutable.ArrayBuffer.empty[ProducerTransactionRecord]
  private val consumerRecords =
    mutable.Map.empty[ConsumerTransactionKey, ConsumerTransactionRecord]

  private def putConsumerTransaction(consumerRecords: mutable.Map[ConsumerTransactionKey, ConsumerTransactionRecord],
                                     consumerTransactionRecord: ConsumerTransactionRecord): Unit = {
    val txnForUpdateOpt = consumerRecords
      .get(consumerTransactionRecord.key)

    txnForUpdateOpt match {
      case Some(oldTxn)
        if consumerTransactionRecord.timestamp > oldTxn.timestamp =>
        consumerRecords.put(
          consumerTransactionRecord.key,
          consumerTransactionRecord
        )
      case None =>
        consumerRecords.put(
          consumerTransactionRecord.key,
          consumerTransactionRecord
        )
      case _ =>
    }
  }

  private def putProducerTransaction(producerRecords: mutable.ArrayBuffer[ProducerTransactionRecord],
                                     producerTransactionRecord: ProducerTransactionRecord,
                                     token: Int) = {
    producerRecords += producerTransactionRecord

    producerTransactionRecord.state match {
      case TransactionStates.Opened =>
        openedTransactionsCache.add(producerTransactionRecord.transactionID, token)
      case TransactionStates.Checkpointed | TransactionStates.Cancel =>
        openedTransactionsCache.remove(producerTransactionRecord.transactionID)
      case _ =>
    }
  }

  private def decomposeTransaction(producerRecords: mutable.ArrayBuffer[ProducerTransactionRecord],
                                   consumerRecords: mutable.Map[ConsumerTransactionKey, ConsumerTransactionRecord],
                                   transaction: Transaction,
                                   timestamp: Long,
                                   token: Int) = {
    transaction.consumerTransaction.foreach { consumerTransaction =>
      val consumerTransactionRecord =
        ConsumerTransactionRecord(consumerTransaction, timestamp)
      putConsumerTransaction(consumerRecords, consumerTransactionRecord)
    }

    transaction.producerTransaction.foreach { producerTransaction =>
      val producerTransactionRecord =
        ProducerTransactionRecord(producerTransaction, timestamp)
      putProducerTransaction(producerRecords, producerTransactionRecord, token)
    }
  }

  def commit(): Boolean = {
    bigCommit.putProducerTransactions(
      producerRecords.sorted
    )

    bigCommit.putConsumerTransactions(
      consumerRecords.values.toIndexedSeq
    )

    producerRecords.clear()
    consumerRecords.clear()

    bigCommit.commit()
  }

  def addFrames(frames: Seq[Frame]): Unit = {
    frames.foreach { frame =>
      frame.typeId match {
        case Frame.PutTransactionDataType =>
          val producerData = Frame.deserializePutTransactionData(frame.body)

          bigCommit.putProducerData(
            producerData.streamID,
            producerData.partition,
            producerData.transaction,
            producerData.data,
            producerData.from)


        case Frame.PutSimpleTransactionAndDataType =>
          val producerTransactionsAndData = Frame.deserializePutSimpleTransactionAndData(frame.body)

          val producerTransactionRecords = producerTransactionsAndData.producerTransactions
            .map(producerTransaction =>
              ProducerTransactionRecord(
                producerTransaction,
                frame.timestamp))

          val producerTransactionRecord =
            producerTransactionRecords.head

          bigCommit.putProducerData(
            producerTransactionRecord.stream,
            producerTransactionRecord.partition,
            producerTransactionRecord.transactionID,
            producerTransactionsAndData.data,
            0)

          producerTransactionRecords.foreach(
            producerTransactionRecord => putProducerTransaction(producerRecords, producerTransactionRecord, frame.token))


        case Frame.PutProducerStateWithDataType =>
          val producerTransactionAndData = Frame.deserializePutProducerStateWithData(frame.body)

          val producerTransactionRecord = ProducerTransactionRecord(
            producerTransactionAndData.transaction,
            frame.timestamp)

          bigCommit.putProducerData(
            producerTransactionRecord.stream,
            producerTransactionRecord.partition,
            producerTransactionRecord.transactionID,
            producerTransactionAndData.data,
            producerTransactionAndData.from)

          putProducerTransaction(producerRecords, producerTransactionRecord, frame.token)


        case Frame.PutConsumerCheckpointType =>
          val consumerTransactionArgs = Frame.deserializePutConsumerCheckpoint(frame.body)
          val consumerTransactionRecord = {
            import consumerTransactionArgs._
            ConsumerTransactionRecord(
              name,
              streamID,
              partition,
              transaction,
              frame.timestamp)
          }
          putConsumerTransaction(consumerRecords, consumerTransactionRecord)


        case Frame.PutTransactionType =>
          val transaction = Frame.deserializePutTransaction(frame.body).transaction
          decomposeTransaction(producerRecords, consumerRecords, transaction, frame.timestamp, frame.token)


        case Frame.PutTransactionsType =>
          val transactions = Frame.deserializePutTransactions(frame.body).transactions

          transactions.foreach(transaction =>
            decomposeTransaction(producerRecords, consumerRecords, transaction, frame.timestamp, frame.token))

        case Frame.TokenCreatedType | Frame.TokenUpdatedType =>
          openedTransactionsCache.tokenCache.add(frame.token, frame.timestamp)

        case Frame.TokenExpiredType =>
          openedTransactionsCache.tokenCache.remove(frame.token)

        case _ =>
      }
    }
  }
}
