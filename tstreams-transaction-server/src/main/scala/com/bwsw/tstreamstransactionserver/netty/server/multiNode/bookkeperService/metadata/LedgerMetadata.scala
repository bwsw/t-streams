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
