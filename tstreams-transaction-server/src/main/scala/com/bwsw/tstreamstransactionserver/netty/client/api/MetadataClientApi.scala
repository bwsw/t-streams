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

package com.bwsw.tstreamstransactionserver.netty.client.api

import com.bwsw.tstreamstransactionserver.rpc.{ScanTransactionsInfo, TransactionInfo, TransactionStates}

import scala.concurrent.{Future => ScalaFuture}

trait MetadataClientApi {
  def getTransaction(): ScalaFuture[Long]

  def getTransaction(timestamp: Long): ScalaFuture[Long]

  def putTransactions(producerTransactions: Seq[com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction],
                      consumerTransactions: Seq[com.bwsw.tstreamstransactionserver.rpc.ConsumerTransaction]): ScalaFuture[Boolean]

  def putProducerState(transaction: com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction): ScalaFuture[Boolean]

  def putTransaction(transaction: com.bwsw.tstreamstransactionserver.rpc.ConsumerTransaction): ScalaFuture[Boolean]

  def openTransaction(streamID: Int, partitionID: Int, transactionTTLMs: Long): ScalaFuture[Long]

  def getTransaction(streamID: Int, partition: Int, transaction: Long): ScalaFuture[TransactionInfo]

  def getLastCheckpointedTransaction(streamID: Int, partition: Int): ScalaFuture[Long]

  def scanTransactions(streamID: Int, partition: Int, from: Long, to: Long, count: Int, states: Set[TransactionStates]): ScalaFuture[ScanTransactionsInfo]
}
