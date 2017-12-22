
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

package com.bwsw.tstreamstransactionserver.netty

import com.bwsw.tstreamstransactionserver.tracing.TracingHeaders
import io.netty.buffer.{ByteBuf, ByteBufAllocator}


/** Message is a placeholder for some binary information.
  *
  * @constructor create a new message with body, the size of body, and a protocol to serialize/deserialize the body.
  * @param bodyLength     a size of body.
  * @param thriftProtocol a protocol to serialize/deserialize the body.
  * @param body           a binary representation of information.
  *
  */
case class RequestMessage(id: Long,
                          bodyLength: Int,
                          thriftProtocol: Byte,
                          body: Array[Byte],
                          token: Int,
                          methodId: Byte,
                          isFireAndForgetMethod: Boolean,
                          tracingInfo: Option[TracingHeaders] = None) {
  /** Serializes a message. */
  def toByteArray: Array[Byte] = {
    val tracingBytes = tracingInfo.map(_.toByteArray)
    val length = tracingBytes.map(_.length).getOrElse(0) + bodyLength
    val buffer = java.nio.ByteBuffer
      .allocate(size)
      .putLong(id) //0-8
      .put(thriftProtocol) //8-9
      .putInt(token) //9-13
      .put(methodId) //13-14
      .put(flags) //14-15
      .putInt(length) //15-19

    tracingBytes.foreach(buffer.put)

    buffer
      .put(body)
      .flip()

    if (buffer.hasArray) {
      buffer.array()
    }
    else {
      val binaryMessage = new Array[Byte](size)
      buffer.get(binaryMessage)
      binaryMessage
    }
  }

  def toByteBuf(byteBufAllocator: ByteBufAllocator): ByteBuf = {
    val tracingBytes = tracingInfo.map(_.toByteArray)
    val length = tracingBytes.map(_.length).getOrElse(0) + bodyLength

    val buffer = byteBufAllocator
      .buffer(size, size)
      .writeLong(id)
      .writeByte(thriftProtocol)
      .writeInt(token)
      .writeByte(methodId)
      .writeByte(flags)
      .writeInt(length)

    tracingBytes.foreach(buffer.writeBytes)

    buffer.writeBytes(body)
  }

  private def size = RequestMessage.headerFieldSize +
    RequestMessage.lengthFieldSize +
    body.length +
    tracingInfo.map(_ => TracingHeaders.sizeInBytes).getOrElse(0)

  private def flags: Byte = {
    var flags = 0
    if (isFireAndForgetMethod)
      flags |= RequestMessage.isFireAndForgetFlag
    if (tracingInfo.isDefined)
      flags |= RequestMessage.isTracedFlag

    flags.toByte
  }
}

object RequestMessage {
  val headerFieldSize: Byte = (
    java.lang.Long.BYTES + //id
      java.lang.Byte.BYTES + //protocol
      java.lang.Integer.BYTES + //token
      java.lang.Byte.BYTES + //method
      java.lang.Byte.BYTES //flags
    ).toByte
  val lengthFieldSize = java.lang.Integer.BYTES //length

  private val isTracedFlag: Byte = 1
  private val isFireAndForgetFlag: Byte = 2

  /** Deserializes a binary to message. */
  def fromByteArray(bytes: Array[Byte]): RequestMessage = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)
    val id = buffer.getLong()
    val protocol = buffer.get()
    val token = buffer.getInt()
    val method = buffer.get()
    val flags = buffer.get()
    var length = buffer.getInt()

    val tracingInfo =
      if (isTraced(flags)) {
        val bytes = new Array[Byte](TracingHeaders.sizeInBytes)
        buffer.get(bytes)
        length -= TracingHeaders.sizeInBytes

        Some(TracingHeaders.fromByteArray(bytes))
      }
      else None

    val message = {
      val bytes = new Array[Byte](length)
      buffer.get(bytes)
      bytes
    }

    RequestMessage(id, length, protocol, message, token, method, isFireAndForgetMethod(flags), tracingInfo)
  }

  def fromByteBuf(buf: ByteBuf): RequestMessage = {
    val id = buf.readLong()
    val protocol = buf.readByte()
    val token = buf.readInt()
    val method = buf.readByte()
    val flags = buf.readByte()
    var length = buf.readInt()

    val tracingInfo =
      if (isTraced(flags)) {
        val bytes = new Array[Byte](TracingHeaders.sizeInBytes)
        buf.readBytes(bytes)
        length -= TracingHeaders.sizeInBytes

        Some(TracingHeaders.fromByteArray(bytes))
      }
      else None

    val message = {
      val bytes = new Array[Byte](length)
      buf.slice()
      buf.readBytes(bytes)
      bytes
    }

    RequestMessage(id, length, protocol, message, token, method, isFireAndForgetMethod(flags), tracingInfo)
  }

  def getIdFromByteBuf(buf: ByteBuf): Long = {
    buf.getLong(0)
  }

  def isFireAndForgetMethod(flags: Byte): Boolean =
    (flags & isFireAndForgetFlag) != 0

  def isTraced(flags: Byte): Boolean =
    (flags & isTracedFlag) != 0
}

