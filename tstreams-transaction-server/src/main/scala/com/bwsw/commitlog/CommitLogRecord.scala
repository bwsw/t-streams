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
package com.bwsw.commitlog

import java.nio.ByteBuffer

import com.bwsw.tstreamstransactionserver.netty.server.batch.Frame

import scala.util.{Failure, Success, Try}

final case class CommitLogRecord(messageType: Byte,
                                 timestamp: Long,
                                 token: Int,
                                 message: Array[Byte]) {

  val size: Int = CommitLogRecord.headerSize + message.length

  @inline
  def toByteArray: Array[Byte] = {
    val buffer = ByteBuffer.allocate(size)
      .put(messageType)
      .putLong(timestamp)
      .putInt(token)
      .put(message)

    buffer.flip()

    if (buffer.hasArray)
      buffer.array()
    else {
      val bytes = new Array[Byte](size)
      buffer.get(bytes)
      bytes
    }
  }


  override def equals(obj: scala.Any): Boolean = obj match {
    case commitLogRecord: CommitLogRecord =>
      messageType == commitLogRecord.messageType &&
        timestamp == commitLogRecord.timestamp &&
        token == commitLogRecord.token &&
        message.sameElements(commitLogRecord.message)
    case _ => false
  }

  def toFrame: Frame = {
    new Frame(
      messageType,
      timestamp,
      token,
      message)
  }
}


object CommitLogRecord {

  val headerSize: Int =
    java.lang.Byte.BYTES + // messageType
      java.lang.Long.BYTES + // timestamp
      java.lang.Integer.BYTES // token

  def fromByteArray(bytes: Array[Byte]): Either[IllegalArgumentException, CommitLogRecord] = {
    Try {
      val buffer = ByteBuffer.wrap(bytes)
      val messageType = buffer.get()
      val timestamp = buffer.getLong()
      val token = buffer.getInt()

      val message = {
        val binaryMessage = new Array[Byte](buffer.remaining())
        buffer.get(binaryMessage)
        binaryMessage
      }

      CommitLogRecord(messageType, timestamp, token, message)
    } match {
      case Success(record) =>
        Right(record)
      case Failure(_) =>
        Left(new IllegalArgumentException("Commit log record is corrupted"))
    }
  }
}
