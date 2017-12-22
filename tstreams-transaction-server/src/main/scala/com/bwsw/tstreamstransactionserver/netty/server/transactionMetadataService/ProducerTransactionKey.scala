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


case class ProducerTransactionKey(stream: Int,
                                  partition: Int,
                                  transactionID: Long)
  extends Ordered[ProducerTransactionKey] {

  override def compare(that: ProducerTransactionKey): Int = {
    if (this.stream < that.stream) -1
    else if (this.stream > that.stream) 1
    else if (this.partition < that.partition) -1
    else if (this.partition > that.partition) 1
    else if (this.transactionID < that.transactionID) -1
    else if (this.transactionID > that.transactionID) 1
    else 0
  }

  def toByteArray: Array[Byte] = {
    val size = ProducerTransactionKey.sizeInBytes

    val buffer = java.nio.ByteBuffer.allocate(size)
      .putInt(stream)
      .putInt(partition)
      .putLong(transactionID)
    buffer.flip()

    if (buffer.hasArray)
      buffer.array()
    else {
      val bytes = new Array[Byte](size)
      buffer.get(bytes)
      bytes
    }
  }
}

object ProducerTransactionKey {
  private val sizeInBytes = java.lang.Integer.BYTES +
    java.lang.Integer.BYTES +
    java.lang.Long.BYTES

  def fromByteArray(bytes: Array[Byte]): ProducerTransactionKey = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)
    val stream = buffer.getInt
    val partition = buffer.getInt
    val transactionID = buffer.getLong
    ProducerTransactionKey(stream, partition, transactionID)
  }
}




