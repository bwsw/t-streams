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
package com.bwsw.tstreamstransactionserver.netty.server.consumerService

import com.bwsw.tstreamstransactionserver.rpc.ConsumerTransaction


object ConsumerTransactionRecord {
  def apply(txn: ConsumerTransaction, timestamp: Long): ConsumerTransactionRecord = {
    val key = ConsumerTransactionKey(txn.name, txn.stream, txn.partition)
    val value = ConsumerTransactionValue(txn.transactionID, timestamp)
    ConsumerTransactionRecord(key, value)
  }

  def apply(name: String,
            streamID: Int,
            partition: Int,
            transactionID: Long,
            timestamp: Long): ConsumerTransactionRecord = {
    new ConsumerTransactionRecord(
      name,
      streamID,
      partition,
      transactionID,
      timestamp
    )
  }
}

case class ConsumerTransactionRecord(key: ConsumerTransactionKey,
                                     consumerTransaction: ConsumerTransactionValue)
  extends ConsumerTransaction
    with Ordered[ConsumerTransactionRecord] {
  def this(name: String,
           streamID: Int,
           partition: Int,
           transactionID: Long,
           timestamp: Long) = {
    this(
      ConsumerTransactionKey(name, streamID, partition),
      ConsumerTransactionValue(transactionID, timestamp)
    )
  }

  override def compare(that: ConsumerTransactionRecord): Int = {
    if (this.timestamp < that.timestamp) -1
    else if (this.timestamp > that.timestamp) 1
    else if (this.name < that.name) -1
    else if (this.name > that.name) 1
    else if (this.stream < that.stream) -1
    else if (this.stream > that.stream) 1
    else if (this.partition < that.partition) -1
    else if (this.partition > that.partition) 1
    else if (this.transactionID < that.transactionID) -1
    else if (this.transactionID > that.transactionID) 1
    else 0
  }

  override def transactionID: Long = consumerTransaction.transactionId

  override def name: String = key.name

  override def stream: Int = key.streamID

  override def partition: Int = key.partition

  def timestamp: Long = Long2long(consumerTransaction.timestamp)
}
