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

package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.storage

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.LedgerHandle
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.{Record, RecordWithIndex}

class BookKeeperLedgerHandleWrapper(ledgerHandler: org.apache.bookkeeper.client.LedgerHandle)
  extends LedgerHandle(ledgerHandler.getId) {

  override def addRecord(data: Record): Long = {
    val bytes = data.toByteArray
    ledgerHandler.addEntry(bytes)
  }

  override def getRecord(id: Long): Record = {
    val entry = ledgerHandler.readEntries(id, id)
    if (entry.hasMoreElements)
      Record.fromByteArray(entry.nextElement().getEntry)
    else
      null: Record
  }

  override def getOrderedRecords(from: Long): Array[RecordWithIndex] = {
    val lo = math.max(from, 0)
    val hi = lastRecordID()

    val indexes = lo to hi
    readRecords(lo, hi)
      .zip(indexes).sortBy(_._1.timestamp)
      .map { case (record, index) =>
        RecordWithIndex(index, record)
      }
  }

  override def readRecords(from: Long, to: Long): Array[Record] = {
    val lo = math.max(from, 0)
    val hi = math.min(math.max(to, 0), lastRecordID())
    val size = math.max(hi - lo + 1, 0).toInt

    if (hi < lo)
      Array.empty[Record]
    else {
      val records = new Array[Record](size)

      val entries = ledgerHandler.readEntries(lo, hi)
      var index = 0
      while (entries.hasMoreElements) {
        records(index) = Record.fromByteArray(entries.nextElement().getEntry)
        index = index + 1
      }
      records
    }
  }

  override def lastRecordID(): Long =
    ledgerHandler.getLastAddConfirmed

  override def lastRecord(): Option[Record] = {
    val lastID = lastRecordID()
    val entry = ledgerHandler.readEntries(lastID, lastID)
    if (entry.hasMoreElements)
      Some(Record.fromByteArray(entry.nextElement().getEntry))
    else
      None
  }

  override def close(): Unit =
    ledgerHandler.close()


  override lazy val getCreationTime: Long = {
    val time =
      ledgerHandler.getCustomMetadata.get(LedgerHandle.KeyTime)
    val buffer = java.nio.ByteBuffer.wrap(time)
    buffer.getLong
  }
}
