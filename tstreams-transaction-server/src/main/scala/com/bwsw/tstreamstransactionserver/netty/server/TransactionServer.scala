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
package com.bwsw.tstreamstransactionserver.netty.server

import java.nio.ByteBuffer

import com.bwsw.tstreamstransactionserver.exception.Throwable.StreamDoesNotExist
import com.bwsw.tstreamstransactionserver.netty.server.consumerService.ConsumerTransactionRecord
import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDbBatch
import com.bwsw.tstreamstransactionserver.netty.server.streamService.{StreamRepository, StreamService}
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService._
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler.LastTransaction
import com.bwsw.tstreamstransactionserver.rpc
import com.bwsw.tstreamstransactionserver.rpc._

import scala.collection.Set


class TransactionServer(streamRepository: StreamRepository,
                        rocksWriter: RocksWriter,
                        rocksReader: RocksReader) {

  private val streamService = new StreamService(
    streamRepository
  )

  private val transactionIDService =
    com.bwsw.tstreamstransactionserver.netty.server.transactionIDService.TransactionIdService
  
  final def putStream(stream: String, partitions: Int, description: Option[String], ttl: Long): Int =
    streamService.putStream(stream, partitions, description, ttl)

  final def checkStreamExists(name: String): Boolean =
    streamService.checkStreamExists(name)

  final def getStream(name: String): Option[rpc.Stream] =
    streamService.getStream(name)

  final def delStream(name: String): Boolean =
    streamService.delStream(name)

  final def getTransactionID: Long =
    transactionIDService.getTransaction()

  final def getTransactionIDByTimestamp(timestamp: Long): Long =
    transactionIDService.getTransaction(timestamp)

  @throws[StreamDoesNotExist]
  final def putTransactionData(streamID: Int, partition: Int, transaction: Long, data: Seq[ByteBuffer], from: Int): Boolean =
    rocksWriter.putTransactionData(streamID, partition, transaction, data, from)

  final def putTransactions(transactions: Seq[ProducerTransactionRecord],
                            batch: KeyValueDbBatch): Unit = {
    rocksWriter.putTransactions(transactions, batch)
  }

  final def getTransaction(streamID: Int, partition: Int, transaction: Long): TransactionInfo =
    rocksReader.getTransaction(streamID, partition, transaction)

  final def getLastCheckpointedTransaction(streamID: Int, partition: Int): Option[Long] =
    rocksReader.getLastTransactionIDAndCheckpointedID(streamID, partition)
      .flatMap(_.checkpointed.map(txn => txn.id)).orElse(Some(-1L))

  final def getLastTransactionIDAndCheckpointedID(streamID: Int, partition: Int): Option[LastTransaction] =
    rocksReader.getLastTransactionIDAndCheckpointedID(streamID, partition)

  final def scanTransactions(streamID: Int, partition: Int, from: Long, to: Long, count: Int, states: Set[TransactionStates]): ScanTransactionsInfo =
    rocksReader.scanTransactions(streamID, partition, from, to, count, states)

  final def getTransactionData(streamID: Int, partition: Int, transaction: Long, from: Int, to: Int): Seq[ByteBuffer] = {
    rocksReader.getTransactionData(streamID, partition, transaction, from, to)
  }

  final def putConsumersCheckpoints(consumerTransactions: Seq[ConsumerTransactionRecord],
                                    batch: KeyValueDbBatch): Unit = {
    rocksWriter.putConsumersCheckpoints(consumerTransactions, batch)
  }

  final def getConsumerState(name: String, streamID: Int, partition: Int): Long = {
    rocksReader.getConsumerState(name, streamID, partition)
  }
}