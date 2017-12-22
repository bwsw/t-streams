package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.metadata

object LedgerMetadataStatus {
  def apply(status: Byte): LedgerMetadataStatus = status match {
    case IsOkayStatus.status =>
      IsOkayStatus
    case NoRecordReadStatus.status =>
      NoRecordReadStatus
    case NoRecordProcessedStatus.status =>
      NoRecordProcessedStatus
    case MoveToNextLedgerStatus.status =>
      MoveToNextLedgerStatus
    case _ =>
      LedgerMetadataStatus(status)
  }
}


abstract class LedgerMetadataStatus(val status: Byte) {
  override def equals(o: scala.Any): Boolean = o match {
    case that: LedgerMetadataStatus =>
      status == that.status
    case _ => false
  }
}

object IsOkayStatus
  extends LedgerMetadataStatus(0)

object NoRecordReadStatus
  extends LedgerMetadataStatus(-1)

object NoRecordProcessedStatus
  extends LedgerMetadataStatus(-2)

object MoveToNextLedgerStatus
  extends LedgerMetadataStatus(-3)
