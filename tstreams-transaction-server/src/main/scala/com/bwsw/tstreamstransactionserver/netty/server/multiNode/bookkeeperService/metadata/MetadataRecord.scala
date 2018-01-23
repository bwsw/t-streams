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

package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService.metadata

object MetadataRecord {
  private val recordsNumberFieldSize =
    java.lang.Integer.BYTES

  def fromByteArray(bytes: Array[Byte]): MetadataRecord = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)

    val recordNumber = buffer.getInt
    val recordSize = LedgerMetadata.sizeInBytes
    val record = new Array[Byte](recordSize)
    val records = Array.fill(recordNumber) {
      buffer.get(record)
      LedgerMetadata.fromByteArray(record)
    }

    MetadataRecord(records)
  }

  def apply(records: Array[LedgerMetadata]): MetadataRecord =
    new MetadataRecord(records)
}

final class MetadataRecord(val records: Array[LedgerMetadata]) {
  def toByteArray: Array[Byte] = {
    import MetadataRecord._
    val size = recordsNumberFieldSize +
      (records.length * LedgerMetadata.sizeInBytes)
    val recordsToBytes = records.flatMap(_.toByteArray)

    val buffer = java.nio.ByteBuffer.allocate(size)
      .putInt(records.length)
      .put(recordsToBytes)
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
    case that: MetadataRecord =>
      this.records.sameElements(that.records)
    case _ => false
  }
}
