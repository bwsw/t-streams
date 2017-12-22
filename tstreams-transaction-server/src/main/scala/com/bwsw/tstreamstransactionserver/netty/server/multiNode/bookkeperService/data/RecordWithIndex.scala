package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data

case class RecordWithIndex(index: Long, record: Record) {
  override def toString: String = s"Index $index, timestamp ${record.timestamp}"
}
