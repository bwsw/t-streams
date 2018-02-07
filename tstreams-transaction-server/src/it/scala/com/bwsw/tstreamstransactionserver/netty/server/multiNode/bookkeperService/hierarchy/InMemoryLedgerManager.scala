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

import java.util.concurrent.atomic.AtomicLong

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService.{LedgerHandle, LedgerManager}

class InMemoryLedgerManager
  extends LedgerManager{

  private val ledgerIDGen = new AtomicLong(0L)

  private val storage =
    new java.util.concurrent.ConcurrentHashMap[Long, LedgerHandle]()

  override def createLedger(timestamp: Long): LedgerHandle = {
    val id = ledgerIDGen.getAndIncrement()

    val previousLedger = storage.computeIfAbsent(id, {id =>
      new InMemoryLedgerHandle(id, timestamp)
    })

    if (previousLedger != null)
      previousLedger
    else
      throw new IllegalArgumentException()
  }

  override def openLedger(id: Long): Option[LedgerHandle] = {
    Option(storage.get(id))
  }

  override def deleteLedger(id: Long): Boolean = {
    Option(storage.remove(id)).isDefined
  }
}
