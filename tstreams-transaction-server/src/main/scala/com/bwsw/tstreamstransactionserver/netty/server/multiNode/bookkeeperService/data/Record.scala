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

package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService.data


import com.bwsw.tstreamstransactionserver.netty.server.batch.Frame

class Record(val recordType: Byte,
             val timestamp: Long,
             val token: Int,
             val body: Array[Byte])
  extends Ordered[Record] {

  def toByteArray: Array[Byte] = {
    val size = Record.BYTES + body.length

    val buffer = java.nio.ByteBuffer.allocate(size)
      .put(recordType)
      .putLong(timestamp)
      .putInt(token)
      .put(body)
    buffer.flip()

    if (buffer.hasArray) {
      buffer.array()
    } else {
      val bytes = new Array[Byte](size)
      buffer.get(bytes)
      bytes
    }
  }

  override def equals(obj: scala.Any): Boolean = obj match {
    case that: Record =>
      recordType == that.recordType &&
        timestamp == that.timestamp &&
        token == that.token &&
        body.sameElements(that.body)
    case _ =>
      false
  }

  override def hashCode(): Int = {
    31 * (
      31 * (
        31 * (
          31 + timestamp.hashCode()
          ) + token.hashCode()
        ) + recordType.hashCode()
      ) + java.util.Arrays.hashCode(body)
  }

  override def compare(that: Record): Int = {
    if (this.timestamp < that.timestamp) -1
    else if (this.timestamp > that.timestamp) 1
    else if (this.recordType < that.recordType) -1
    else if (this.recordType > that.recordType) 1
    else 0
  }

  def toFrame: Frame = {
    new Frame(
      recordType,
      timestamp,
      token,
      body)
  }
}

object Record {
  private val BYTES =
    java.lang.Byte.BYTES + // record type
      java.lang.Long.BYTES + // timestamp
      java.lang.Integer.BYTES // token

  def fromByteArray(bytes: Array[Byte]): Record = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)

    val recordType = buffer.get()
    val timestamp = buffer.getLong()
    val token = buffer.getInt()

    val body = new Array[Byte](buffer.remaining())
    buffer.get(body)

    recordType match {
      case Frame.Timestamp => new TimestampRecord(timestamp)
      case _ => new Record(recordType, timestamp, token, body)
    }
  }
}
