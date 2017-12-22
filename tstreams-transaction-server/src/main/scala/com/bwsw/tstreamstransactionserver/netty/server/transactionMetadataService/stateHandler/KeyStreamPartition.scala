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
package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler


case class KeyStreamPartition(stream: Int,
                              partition: Int) {

  def toByteArray: Array[Byte] = {
    val size = java.lang.Integer.BYTES +
      java.lang.Integer.BYTES

    val buffer = java.nio.ByteBuffer
      .allocate(size)
      .putInt(stream)
      .putInt(partition)
    buffer.flip()

    if (buffer.hasArray) {
      buffer.array()
    }
    else {
      val bytes = new Array[Byte](size)
      buffer.get(bytes)
      bytes
    }
  }
}

object KeyStreamPartition {
  def fromByteArray(bytes: Array[Byte]): KeyStreamPartition = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)
    val stream = buffer.getInt
    val partition = buffer.getInt
    KeyStreamPartition(stream, partition)
  }
}
