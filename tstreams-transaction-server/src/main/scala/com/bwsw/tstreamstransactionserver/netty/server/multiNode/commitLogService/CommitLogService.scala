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

  def getLastProcessedLedgers: Array[LedgerMetadata] = {
    Option(bookkeeperLogDatabase.get(BigCommit.bookkeeperKey))
      .map(MetadataRecord.fromByteArray)
      .map(_.records)
      .getOrElse(Array.empty[LedgerMetadata])
  }

  def getFirstLedgerId: Long = {
    val ledgers = getLastProcessedLedgers
    if (ledgers.isEmpty) {
      -1L
    }
    else {
      val firstLedgerId = ledgers.minBy(_.id).id

      firstLedgerId
    }
  }
}
