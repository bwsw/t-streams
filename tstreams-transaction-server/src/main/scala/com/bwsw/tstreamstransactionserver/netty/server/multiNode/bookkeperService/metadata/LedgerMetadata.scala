package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.metadata

object LedgerMetadata {
  val sizeInBytes: Int =
    java.lang.Long.BYTES * 2 +
      java.lang.Byte.BYTES

  def fromByteArray(bytes: Array[Byte]): LedgerMetadata = {
    val buffer =
      java.nio.ByteBuffer.wrap(bytes)

    LedgerMetadata(
      buffer.getLong(),
      buffer.getLong(),
      LedgerMetadataStatus(buffer.get())
    )
  }

  def apply(ledgerID: Long,
            ledgerLastRecordID: Long,
            ledgerMetadataStatus: LedgerMetadataStatus): LedgerMetadata =
    new LedgerMetadata(
      ledgerID,
      ledgerLastRecordID,
      ledgerMetadataStatus
    )
}


final class LedgerMetadata(val id: Long,
                           val lastRecordID: Long,
                           val metadataStatus: LedgerMetadataStatus) {
  def toByteArray: Array[Byte] = {
    val size = LedgerMetadata.sizeInBytes
    val buffer =
      java.nio.ByteBuffer
        .allocate(size)
        .putLong(id)
        .putLong(lastRecordID)
        .put(metadataStatus.status)
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
    case that: LedgerMetadata =>
      this.id == that.id &&
        this.lastRecordID == that.lastRecordID &&
        this.metadataStatus == that.metadataStatus
    case _ => false
  }

  override def toString: String = s"Ledger id: $id, last record id: $lastRecordID"
}
