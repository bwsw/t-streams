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
package com.bwsw.tstreamstransactionserver.netty.server.transactionIDService

import java.util.concurrent.atomic.AtomicReference
import java.util.function.UnaryOperator

object TransactionIdService
  extends TransactionIdGenerator {
  private val SCALE = 100000

  private val transactionIdAndCurrentTime: AtomicReference[TransactionGeneratorUnit] = {
    val transactionGeneratorUnit =
      TransactionGeneratorUnit(0, 0L)

    new AtomicReference(transactionGeneratorUnit)
  }

  private val update = new UnaryOperator[TransactionGeneratorUnit] {
    override def apply(transactionGenUnit: TransactionGeneratorUnit): TransactionGeneratorUnit = {
      val now = System.currentTimeMillis()
      if ((now - transactionGenUnit.currentTime) > 0L) {
        TransactionGeneratorUnit(0, now)
      } else
        transactionGenUnit.copy(
          transactionId = transactionGenUnit.transactionId + 1
        )
    }
  }

  override def getTransaction(): Long = {
    val txn = transactionIdAndCurrentTime.updateAndGet(update)
    getTransaction(txn.currentTime) + txn.transactionId
  }

  override def getTransaction(timestamp: Long): Long = {
    timestamp * TransactionIdService.SCALE
  }
}
