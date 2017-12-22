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

import com.bwsw.tstreamstransactionserver.rpc.TransactionStates

case class ProducerTransactionValue(state: TransactionStates,
                                    quantity: Int,
                                    ttl: Long,
                                    timestamp: Long)
  extends Ordered[ProducerTransactionValue] {

  def toByteArray: Array[Byte] = {
    val size = ProducerTransactionValue.sizeInBytes

    val buffer = java.nio.ByteBuffer.allocate(size)
      .putInt(state.value)
      .putInt(quantity)
      .putLong(ttl)
      .putLong(timestamp)
    buffer.flip()

    if (buffer.hasArray)
      buffer.array()
    else {
      val bytes = new Array[Byte](size)
      buffer.get(bytes)
      bytes
    }
  }

  override def compare(that: ProducerTransactionValue): Int = {
    if (this.timestamp < that.timestamp) -1
    else if (this.timestamp > that.timestamp) 1
    else if (this.state.value < that.state.value) -1
    else if (this.state.value > that.state.value) 1
    else 0
  }
}

object ProducerTransactionValue {
  private val sizeInBytes = java.lang.Integer.BYTES +
    java.lang.Integer.BYTES +
    java.lang.Long.BYTES +
    java.lang.Long.BYTES

  def fromByteArray(bytes: Array[Byte]): ProducerTransactionValue = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)
    val state = TransactionStates(buffer.getInt)
    val quantity = buffer.getInt
    val ttl = buffer.getLong
    val timestamp = buffer.getLong
    ProducerTransactionValue(state, quantity, ttl, timestamp)
  }
}
