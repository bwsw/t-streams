package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data


import com.bwsw.tstreamstransactionserver.netty.server.batch.Frame

class Record(val recordType: Byte,
             val timestamp: Long,
             val body: Array[Byte])
  extends Ordered[Record] {

  def this(recordType: Frame.Value,
           timestamp: Long,
           body: Array[Byte]) = {
    this(recordType.id.toByte, timestamp, body)
  }

  def toByteArray: Array[Byte] = {
    val size = Record.recordTypeSizeField +
      Record.timestampSizeField +
      body.length

    val buffer = java.nio.ByteBuffer.allocate(size)
      .put(recordType)
      .putLong(timestamp)
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
        body.sameElements(that.body)
    case _ =>
      false
  }

  override def hashCode(): Int = {
    31 * (
      31 * (
        31 + timestamp.hashCode()
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
}

object Record {
  private val recordTypeSizeField = java.lang.Byte.BYTES
  private val timestampSizeField = java.lang.Long.BYTES

  def fromByteArray(bytes: Array[Byte]): Record = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)

    val recordType = buffer.get
    val timestamp  = buffer.getLong

    val body = new Array[Byte](buffer.remaining())
    buffer.get(body)

    val timestampId = Frame.Timestamp.id.toByte
    recordType match {
      case `timestampId` => new TimestampRecord(timestamp)
      case _ => new Record(recordType, timestamp, body)
    }
  }
}
