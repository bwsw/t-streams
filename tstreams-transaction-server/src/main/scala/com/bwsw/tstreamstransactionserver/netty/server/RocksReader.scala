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

import com.bwsw.tstreamstransactionserver.netty.server.consumerService.ConsumerServiceRead
import com.bwsw.tstreamstransactionserver.netty.server.storage.Storage
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataService
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService._
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler.{LastTransaction, LastTransactionReader}
import com.bwsw.tstreamstransactionserver.rpc._

import scala.collection.Set

class RocksReader(storage: Storage,
                  transactionDataService: TransactionDataService) {

  private val consumerService =
    new ConsumerServiceRead(
      storage.getStorageManager
    )

  private val lastTransactionReader =
    new LastTransactionReader(
      storage.getStorageManager
    )

  private val transactionIDService =
    com.bwsw.tstreamstransactionserver.netty.server.transactionIDService.TransactionIdService

  private val transactionMetaServiceReader =
    new TransactionMetaServiceReader(
      storage.getStorageManager
    )

  final def getTransactionID: Long =
    transactionIDService.getTransaction()

  final def getTransactionIDByTimestamp(timestamp: Long): Long =
    transactionIDService.getTransaction(timestamp)

  final def getTransaction(streamID: Int,
                           partition: Int,
                           transaction: Long): TransactionInfo =
    transactionMetaServiceReader.getTransaction(
      streamID,
      partition,
      transaction
    )


  final def getLastTransactionIDAndCheckpointedID(streamID: Int,
                                                  partition: Int): Option[LastTransaction] =
    lastTransactionReader.getLastTransaction(
      streamID,
      partition
    )

  final def scanTransactions(streamID: Int,
                             partition: Int,
                             from: Long,
                             to: Long,
                             count: Int,
                             states: Set[TransactionStates]): ScanTransactionsInfo =
    transactionMetaServiceReader.scanTransactions(
      streamID,
      partition,
      from,
      to,
      count,
      states
    )

  final def getTransactionData(streamID: Int,
                               partition: Int,
                               transaction: Long,
                               from: Int,
                               to: Int): Seq[ByteBuffer] = {
    transactionDataService.getTransactionData(
      streamID,
      partition,
      transaction,
      from,
      to
    )
  }

  final def getConsumerState(name: String,
                             streamID: Int,
                             partition: Int): Long = {
    consumerService.getConsumerState(
      name,
      streamID,
      partition
    )
  }
}
