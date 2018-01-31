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
package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService.storage

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService.{LedgerHandle, LedgerManager}
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions.BookkeeperOptions
import org.apache.bookkeeper.client.BookKeeper

import scala.util.Try

class BookKeeperWrapper(bookKeeper: BookKeeper,
                        bookkeeperOptions: BookkeeperOptions)
  extends LedgerManager {

  override def createLedger(timestamp: Long): LedgerHandle = {

    val metadata =
      new java.util.HashMap[String, Array[Byte]]

    val size =
      java.lang.Long.BYTES
    val buffer =
      java.nio.ByteBuffer
        .allocate(size)
        .putLong(timestamp)
    buffer.flip()

    val bytes = if (buffer.hasArray)
      buffer.array()
    else {
      val bytes = new Array[Byte](size)
      buffer.get(bytes)
      bytes
    }

    metadata.put(LedgerHandle.KeyTime, bytes)

    val ledgerHandle = bookKeeper.createLedger(
      bookkeeperOptions.ensembleNumber,
      bookkeeperOptions.writeQuorumNumber,
      bookkeeperOptions.ackQuorumNumber,
      BookKeeper.DigestType.MAC,
      bookkeeperOptions.password,
      metadata
    )
    new BookKeeperLedgerHandle(ledgerHandle)
  }

  override def openLedger(id: Long): Option[LedgerHandle] = {
    val ledgerHandleTry = Try(bookKeeper.openLedgerNoRecovery(
      id,
      BookKeeper.DigestType.MAC,
      bookkeeperOptions.password
    ))
    ledgerHandleTry.map(ledgerHandle =>
      new BookKeeperLedgerHandle(ledgerHandle)
    ).toOption
  }

  override def deleteLedger(id: Long): Boolean = {
    Try(bookKeeper.deleteLedger(id)).isSuccess
  }
}
