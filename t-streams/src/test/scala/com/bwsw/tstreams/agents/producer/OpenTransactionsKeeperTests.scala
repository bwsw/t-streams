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

package com.bwsw.tstreams.agents.producer


import com.bwsw.tstreams.agents.group.ProducerTransactionState
import com.bwsw.tstreams.testutils.IncreasingGenerator
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 28.08.16.
  */
class OpenTransactionsKeeperTests extends FlatSpec with Matchers {

  var ctr: Int = 0

  class TransactionStub extends ProducerTransaction {
    var lastMethod: String = null

    override def finalizeDataSend(): Unit = {}

    override def cancel(): Unit = {
      lastMethod = "cancel"
    }

    override def isClosed(): Boolean = false

    override def checkpoint(): Unit = {
      lastMethod = "checkpoint"
    }

    override def notifyUpdate(): Unit = {
      ctr += 1
    }

    override def getStateInfo(status: TransactionStates): ProducerTransactionState = null

    override def getTransactionID(): Long = IncreasingGenerator.get

    override def markAsClosed(): Unit = {}

    override def send(obj: Array[Byte]): ProducerTransaction = null

    override private[tstreams] def getUpdateInfo(): Option[RPCProducerTransaction] = None

    override private[tstreams] def getCancelInfoAndClose(): Option[RPCProducerTransaction] = None

    override private[tstreams] def notifyCancelEvent(): Unit = {}
  }

  class TransactionStubBadTransactionID extends TransactionStub {
    override def getTransactionID(): Long = 0
  }

  it should "allow add and get transactions to it" in {
    val keeper = new OpenTransactionsKeeper()
    keeper.put(0, new TransactionStub)
    keeper.getTransactionSetOption(0).isDefined shouldBe true
    keeper.getTransactionSetOption(1).isDefined shouldBe false
  }

  it should "handle all variants of awaitMaterialized" in {
    val keeper = new OpenTransactionsKeeper()
    val t = new TransactionStub
    keeper.put(0, t)
    keeper.handlePreviousOpenTransaction(0, NewProducerTransactionPolicy.CheckpointIfOpened)()
    t.lastMethod shouldBe "checkpoint"
    keeper.handlePreviousOpenTransaction(0, NewProducerTransactionPolicy.CancelIfOpened)()
    t.lastMethod shouldBe "cancel"
    (try {
      keeper.handlePreviousOpenTransaction(0, NewProducerTransactionPolicy.ErrorIfOpened)()
      false
    } catch {
      case e: IllegalStateException =>
        true
    }) shouldBe true
  }

  it should "handle for all keys do properly" in {
    val keeper = new OpenTransactionsKeeper()
    keeper.put(0, new TransactionStub)
    keeper.put(1, new TransactionStub)
    keeper.put(2, new TransactionStub)
    keeper.forallTransactionsDo[Unit]((p: Int, t: ProducerTransaction) => t.notifyUpdate())
    ctr shouldBe 3
  }


}
