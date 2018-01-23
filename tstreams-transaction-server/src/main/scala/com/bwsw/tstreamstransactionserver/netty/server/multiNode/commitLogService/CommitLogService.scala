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

package com.bwsw.tstreamstransactionserver.netty.server.multiNode.commitLogService

import com.bwsw.tstreamstransactionserver.netty.server.batch.BigCommit
import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDbManager
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService.metadata.{LedgerMetadata, MetadataRecord}
import com.bwsw.tstreamstransactionserver.netty.server.storage.Storage

final class CommitLogService(rocksDB: KeyValueDbManager) {
  private val bookkeeperLogDatabase = rocksDB.getDatabase(Storage.BOOKKEEPER_LOG_STORE)

  //TODO rename function
  def getLastProcessedLedgersAndRecordIDs: Array[LedgerMetadata] = {
    Option(bookkeeperLogDatabase.get(BigCommit.bookkeeperKey))
      .map(MetadataRecord.fromByteArray)
      .map(_.records)
      .getOrElse(Array.empty[LedgerMetadata])
  }


  def getMinMaxLedgersIds: MinMaxLedgerIDs = {
    val ledgers = getLastProcessedLedgersAndRecordIDs
    if (ledgers.isEmpty) {
      MinMaxLedgerIDs(-1L, -1L)
    }
    else {
      val min = ledgers.minBy(_.id).id
      val max = ledgers.maxBy(_.id).id
      MinMaxLedgerIDs(min, max)
    }
  }
}
