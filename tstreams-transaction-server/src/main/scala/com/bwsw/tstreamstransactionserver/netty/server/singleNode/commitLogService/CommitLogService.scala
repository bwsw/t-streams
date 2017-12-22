package com.bwsw.tstreamstransactionserver.netty.server.singleNode.commitLogService

import com.bwsw.tstreamstransactionserver.netty.server.batch.BigCommit
import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDbManager
import com.bwsw.tstreamstransactionserver.netty.server.storage.Storage
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.CommitLogKey

class CommitLogService(rocksDB: KeyValueDbManager) {
  private val commitLogDatabase = rocksDB.getDatabase(Storage.COMMIT_LOG_STORE)

  private[server] final def getLastProcessedCommitLogFileID: Long = {
    Option(commitLogDatabase.get(BigCommit.commitLogKey))
      .map(CommitLogKey.fromByteArray)
      .map(_.id)
      .getOrElse(-1L)
  }
}
