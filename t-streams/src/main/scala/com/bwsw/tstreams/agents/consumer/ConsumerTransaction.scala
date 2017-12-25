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

package com.bwsw.tstreams.agents.consumer

import com.bwsw.tstreamstransactionserver.rpc.TransactionStates

import scala.collection.mutable


/**
  *
  * @param partition
  * @param transactionID
  * @param count
  * @param state
  * @param ttl
  */
class ConsumerTransaction(partition: Int,
                          transactionID: Long,
                          count: Int,
                          state: TransactionStates,
                          ttl: Long) {

  override def toString: String = {
    s"consumer.Transaction(id=$transactionID,partition=$partition, count=$count, ttl=$ttl)"
  }

  private var _consumer: Option[Consumer] = None

  def consumer: Consumer =
    _consumer.getOrElse(throw new IllegalStateException("The transaction is not yet attached to consumer"))

  def attach(c: Consumer) = {
    if (c == null)
      throw new IllegalArgumentException("Argument must be not null.")

    if (_consumer.isEmpty)
      _consumer = Some(c)
    else
      throw new IllegalStateException("The transaction is already attached to consumer")
  }

  def getTransactionID = transactionID

  def getPartition = partition

  def getCount = count

  def getTTL = ttl

  def getState = state

  /**
    * Transaction data pointer
    */
  private var cnt = 0

  /**
    * Buffer to preload some amount of current transaction data
    */
  private val buffer = mutable.Queue[Array[Byte]]()

  /**
    * @return Next piece of data from current transaction
    */
  def next() = this.synchronized {

    if (consumer == null)
      throw new IllegalArgumentException("Transaction is not yet attached to consumer. Attach it first.")

    if (!hasNext)
      throw new IllegalStateException("There is no data to receive from data storage")

    //try to update buffer
    if (buffer.isEmpty) {
      val newCount = (cnt + consumer.options.dataPreload).min(count - 1)
      buffer ++= consumer.stream.client.getTransactionData(consumer.stream.id, partition, transactionID, cnt, newCount)
      cnt = newCount + 1
    }

    buffer.dequeue()
  }

  /**
    * Indicate consumed or not current transaction
    *
    * @return
    */
  def hasNext: Boolean = this.synchronized {
    cnt < count || buffer.nonEmpty
  }

  /**
    * Refresh BasicConsumerTransaction iterator to read from the beginning
    */
  def replay(): Unit = this.synchronized {
    buffer.clear()
    cnt = 0
  }

  /**
    * @return All consumed transaction
    */
  def getAll = this.synchronized {
    if (consumer == null)
      throw new IllegalArgumentException("Transaction is not yet attached to consumer. Attach it first.")
    val r = consumer.stream.client.getTransactionData(consumer.stream.id, partition, transactionID, cnt, count)

    if (Consumer.logger.isDebugEnabled()) {
      Consumer.logger.debug(s"ConsumerTransaction.getAll(${consumer.stream.name}, $partition, $transactionID, $cnt, ${count - 1})")
      Consumer.logger.debug(s"ConsumerTransaction.getAll: $r")
    }

    mutable.Queue[Array[Byte]]() ++ r

  }

}
