package com.bwsw.tstreamstransactionserver.netty

import com.bwsw.tstreamstransactionserver.netty.ResponseMessage._
import io.netty.buffer.{ByteBuf, ByteBufAllocator}

case class ResponseMessage(id: Long, body: Array[Byte]) {
  /** Serializes a message. */
  def toByteArray: Array[Byte] = {
    val length = body.length

    val size =
      headerFieldSize +
        lengthFieldSize +
        length

    val buffer =
      java.nio.ByteBuffer
        .allocate(size)
        .putLong(id)
        .putInt(length)
        .put(body)
    buffer.flip()

    if (buffer.hasArray) {
      buffer.array()
    } else {
      val array = new Array[Byte](size)
      buffer.get(array)
      array
    }
  }

  def toByteBuf(byteBufAllocator: ByteBufAllocator): ByteBuf = {
    val length = body.length

    val size =
      headerFieldSize +
        lengthFieldSize +
        length

    byteBufAllocator
      .buffer(size, size)
      .writeLong(id)
      .writeInt(length)
      .writeBytes(body)
  }
}

object ResponseMessage {
  val headerFieldSize: Int =
    java.lang.Long.BYTES //id

  val lengthFieldSize =
    java.lang.Integer.BYTES //length

  def fromByteArray(bytes: Array[Byte]): ResponseMessage = {
    val buffer = java.nio.ByteBuffer
      .wrap(bytes)

    val id = buffer.getLong
    val bodyLength = buffer.getInt
    val body = {
      val bytes = new Array[Byte](bodyLength)
      buffer.get(bytes)
      bytes
    }

    ResponseMessage(id, body)
  }

  def fromByteBuf(buf: ByteBuf): ResponseMessage = {
    val id = buf.readLong()
    val bodyLength = buf.readInt()

    val bodyInBytes = {
      if (buf.hasArray) {
        val bytes = buf.array()
        bytes.slice(buf.readerIndex(), bytes.length)
      }
      else {
        val bytes = new Array[Byte](bodyLength)
        buf.slice()
        buf.readBytes(bytes)
        bytes
      }
    }

    ResponseMessage(id, bodyInBytes)
  }

  def getIdFromByteBuf(buf: ByteBuf): Long = {
    buf.getLong(0)
  }
}
