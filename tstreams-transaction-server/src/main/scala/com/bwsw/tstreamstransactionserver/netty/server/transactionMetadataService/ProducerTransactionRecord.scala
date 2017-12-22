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

import com.bwsw.tstreamstransactionserver.rpc.{ProducerTransaction, TransactionStates}

case class ProducerTransactionRecord(key: ProducerTransactionKey,
                                     producerTransaction: ProducerTransactionValue)
  extends ProducerTransaction
    with Ordered[ProducerTransactionRecord] {
  override def quantity: Int = producerTransaction.quantity

  override def ttl: Long = producerTransaction.ttl

  def this(stream: Int,
           partition: Int,
           transactionID: Long,
           state: TransactionStates,
           quantity: Int,
           ttl: Long,
           timestamp: Long) = {
    this(
      ProducerTransactionKey(stream, partition, transactionID),
      ProducerTransactionValue(state, quantity, ttl, timestamp)
    )
  }

  override def compare(that: ProducerTransactionRecord): Int = {
    if (this.timestamp < that.timestamp) -1
    else if (this.timestamp > that.timestamp) 1
    else if (this.stream < that.stream) -1
    else if (this.stream > that.stream) 1
    else if (this.partition < that.partition) -1
    else if (this.partition > that.partition) 1
    else if (this.transactionID < that.transactionID) -1
    else if (this.transactionID > that.transactionID) 1
//    else if (this.state.value < that.state.value) -1
//    else if (this.state.value > that.state.value) 1
    else 0
  }

  override def stream: Int = key.stream

  override def partition: Int = key.partition

  override def transactionID: Long = key.transactionID

  override def state: TransactionStates = producerTransaction.state

  def timestamp: Long = producerTransaction.timestamp

  override def toString: String = {
    s"Producer transaction: " +
      s"stream ${this.key.stream}," +
      s"partition ${this.key.partition}," +
      s"id ${this.key.transactionID}," +
      s"state ${this.producerTransaction.state.name}," +
      s"timestamp ${this.producerTransaction.timestamp}"
  }
}

object ProducerTransactionRecord {
  def apply(txn: ProducerTransaction,
            timestamp: Long): ProducerTransactionRecord = {
    val key = ProducerTransactionKey(txn.stream, txn.partition, txn.transactionID)
    val producerTransaction = ProducerTransactionValue(txn.state, txn.quantity, txn.ttl, timestamp)
    ProducerTransactionRecord(key, producerTransaction)
  }

  def apply(stream: Int,
            partition: Int,
            transactionID: Long,
            state: TransactionStates,
            quantity: Int,
            ttl: Long,
            timestamp: Long): ProducerTransactionRecord = {
    new ProducerTransactionRecord(
      stream,
      partition,
      transactionID,
      state,
      quantity,
      ttl,
      timestamp
    )
  }
}

