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
package com.bwsw.tstreamstransactionserver.netty.server.transactionDataService

import com.bwsw.tstreamstransactionserver.`implicit`.Implicits.intToByteArray


case class Key(partition: Int, transaction: Long) {
  lazy private val binaryKey: Array[Byte] = toByteArray()

  final def toByteArray(): Array[Byte] = {
    val size = java.lang.Integer.BYTES + java.lang.Long.BYTES
    val buffer = java.nio.ByteBuffer
      .allocate(size)
      .putInt(partition)
      .putLong(transaction)
    buffer.flip()

    val binaryArray = new Array[Byte](size)
    buffer.get(binaryArray)
    binaryArray
  }

  final def toByteArray(dataID: Int): Array[Byte] = binaryKey ++: intToByteArray(dataID)

  override def toString: String = s"$partition $transaction"
}

object Key {
  val size = 8
}