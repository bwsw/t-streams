package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy


import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.metadata.{LedgerMetadata, MetadataRecord}
import com.bwsw.tstreamstransactionserver.netty.server.storage.Storage
import com.bwsw.tstreamstransactionserver.netty.server._
import com.bwsw.tstreamstransactionserver.netty.server.batch.{BigCommit, BigCommitWithFrameParser}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.BookkeeperRecordFrame
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.commitLogService.CommitLogService


class BookkeeperToRocksWriter(zkMultipleTreeListReader: ZkMultipleTreeListReader,
                              commitLogService: CommitLogService,
                              rocksWriter: RocksWriter)
  extends Runnable {
  private def getBigCommit(processedLastRecordIDsAcrossLedgers: Array[LedgerMetadata]): BigCommitWithFrameParser = {
    val value = MetadataRecord(processedLastRecordIDsAcrossLedgers).toByteArray
    val bigCommit = new BigCommit(rocksWriter, Storage.BOOKKEEPER_LOG_STORE, BigCommit.bookkeeperKey, value)
    new BigCommitWithFrameParser(bigCommit)
  }

  def processAndPersistRecords(): Boolean = {
    val ledgerRecordIDs = commitLogService
      .getLastProcessedLedgersAndRecordIDs

    val (records, ledgerIDsAndTheirLastRecordIDs) =
      zkMultipleTreeListReader.read(ledgerRecordIDs)

    if (records.isEmpty) {
      false
    }
    else {
      val bigCommit = getBigCommit(ledgerIDsAndTheirLastRecordIDs)
      val frames = records.map(record => new BookkeeperRecordFrame(record))

      bigCommit.addFrames(frames)
      bigCommit.commit()

      rocksWriter.createAndExecuteTransactionsToDeleteTask(
        frames.lastOption
          .map(_.timestamp)
          .getOrElse(System.currentTimeMillis())
      )
      rocksWriter.clearProducerTransactionCache()
      true
    }
  }

  override def run(): Unit = {
    var haveNextRecords = true
    while (haveNextRecords) {
      haveNextRecords = processAndPersistRecords()
    }
  }
}
