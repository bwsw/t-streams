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
