package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.metadata

object MetadataRecord {
  private val recordsNumberFieldSize =
    java.lang.Integer.BYTES

  def fromByteArray(bytes: Array[Byte]): MetadataRecord = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)

    val recordNumber = buffer.getInt
    val recordSize = LedgerMetadata.sizeInBytes
    val record = new Array[Byte](recordSize)
    val records = Array.fill(recordNumber) {
      buffer.get(record)
      LedgerMetadata.fromByteArray(record)
    }

    MetadataRecord(records)
  }

  def apply(records: Array[LedgerMetadata]): MetadataRecord =
    new MetadataRecord(records)
}

final class MetadataRecord(val records: Array[LedgerMetadata]) {
  def toByteArray: Array[Byte] = {
    import MetadataRecord._
    val size = recordsNumberFieldSize +
      (records.length * LedgerMetadata.sizeInBytes)
    val recordsToBytes = records.flatMap(_.toByteArray)

    val buffer = java.nio.ByteBuffer.allocate(size)
      .putInt(records.length)
      .put(recordsToBytes)
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

  override def equals(that: scala.Any): Boolean = that match {
    case that: MetadataRecord =>
      this.records.sameElements(that.records)
    case _ => false
  }
}
