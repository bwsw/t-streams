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

package com.bwsw.tstreams.agents.subscriber


import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import com.bwsw.tstreams.agents.consumer.subscriber.{QueueBuilder, TransactionBuffer}
import com.bwsw.tstreamstransactionserver.protocol.TransactionState
import org.scalatest.{FlatSpec, Matchers}

object TransactionBufferTests {
  val OPENED = 0
  val UPDATE = 1
  val POST = 2
  val CANCEL = 3
  val UPDATE_TTL = 20
  val OPEN_TTL = 1000
  val cntr = new AtomicLong(0)

  def generateAllStates(): Array[TransactionState] = {
    val id = cntr.incrementAndGet()
    Array[TransactionState](
      TransactionState(transactionID = id, partition = 0, masterID = 0, orderID = 0, count = -1, status = TransactionState.Status.Opened, ttlMs = OPEN_TTL),
      TransactionState(transactionID = id, partition = 0, masterID = 0, orderID = 0, count = -1, status = TransactionState.Status.Updated, ttlMs = UPDATE_TTL),
      TransactionState(transactionID = id, partition = 0, masterID = 0, orderID = 0, count = -1, status = TransactionState.Status.Checkpointed, ttlMs = 10),
      TransactionState(transactionID = id, partition = 0, masterID = 0, orderID = 0, count = -1, status = TransactionState.Status.Cancelled, ttlMs = 10))
  }
}

/**
  * Created by Ivan Kudryavtsev on 19.08.16.
  */
class TransactionBufferTests extends FlatSpec with Matchers {

  val OPENED = TransactionBufferTests.OPENED
  val UPDATE = TransactionBufferTests.UPDATE
  val POST = TransactionBufferTests.POST
  val CANCEL = TransactionBufferTests.CANCEL
  val UPDATE_TTL = TransactionBufferTests.UPDATE_TTL
  val OPEN_TTL = TransactionBufferTests.OPEN_TTL

  def generateAllStates() = TransactionBufferTests.generateAllStates()


  it should "be created" in {
    val b = new TransactionBuffer(new QueueBuilder.InMemory().generateQueueObject(0))
  }

  it should "avoid addition of update state if no previous state" in {
    val b = new TransactionBuffer(new QueueBuilder.InMemory().generateQueueObject(0))
    val ts0 = generateAllStates()
    b.updateTransactionState(ts0(UPDATE))
    b.getState(ts0(UPDATE).transactionID).isDefined shouldBe false
  }


  it should "avoid addition of post state if no previous state" in {
    val b = new TransactionBuffer(new QueueBuilder.InMemory().generateQueueObject(0))
    val ts0 = generateAllStates()
    b.updateTransactionState(ts0(POST))
    b.getState(ts0(POST).transactionID).isDefined shouldBe false
  }

  it should "avoid addition of cancel state if no previous state" in {
    val b = new TransactionBuffer(new QueueBuilder.InMemory().generateQueueObject(0))
    val ts0 = generateAllStates()
    b.updateTransactionState(ts0(CANCEL))
    b.getState(ts0(CANCEL).transactionID).isDefined shouldBe false
  }

  it should "avoid addition of ts0 after ts1" in {
    val b = new TransactionBuffer(new QueueBuilder.InMemory().generateQueueObject(0))
    val ts0 = generateAllStates()
    val ts1 = generateAllStates()
    b.updateTransactionState(ts1(OPENED))
    b.updateTransactionState(ts0(OPENED))
    b.getState(ts0(OPENED).transactionID).isDefined shouldBe false
    b.getState(ts1(OPENED).transactionID).isDefined shouldBe true
  }


  it should "allow to place opened state" in {
    val b = new TransactionBuffer(new QueueBuilder.InMemory().generateQueueObject(0))
    val ts0 = generateAllStates()
    b.updateTransactionState(ts0(OPENED))
    b.getState(ts0(OPENED).transactionID).isDefined shouldBe true
  }

  it should "move from opened to updated without problems, state should be opened, ttl must be changed" in {
    val b = new TransactionBuffer(new QueueBuilder.InMemory().generateQueueObject(0))
    val ts0 = generateAllStates()
    b.updateTransactionState(ts0(OPENED))
    b.updateTransactionState(ts0(UPDATE))
    b.getState(ts0(UPDATE).transactionID).isDefined shouldBe true
    Math.abs(b.getState(ts0(UPDATE).transactionID).get.ttlMs - UPDATE_TTL - System.currentTimeMillis()) < 20 shouldBe true
    b.getState(ts0(UPDATE).transactionID).get.status shouldBe TransactionState.Status.Opened
  }

  it should "move from opened to cancelled" in {
    val b = new TransactionBuffer(new QueueBuilder.InMemory().generateQueueObject(0))
    val ts0 = generateAllStates()
    b.updateTransactionState(ts0(OPENED))
    b.updateTransactionState(ts0(CANCEL))
    b.getState(ts0(UPDATE).transactionID).isDefined shouldBe true
  }


  it should "move from opened to checkpoint" in {
    val b = new TransactionBuffer(new QueueBuilder.InMemory().generateQueueObject(0))
    val ts0 = generateAllStates()
    b.updateTransactionState(ts0(OPENED))
    b.updateTransactionState(ts0(POST))
    b.getState(ts0(POST).transactionID).isDefined shouldBe true
    b.getState(ts0(POST).transactionID).get.ttlMs shouldBe Long.MaxValue
  }

  it should "move to checkpoint impossible" in {
    val b = new TransactionBuffer(new QueueBuilder.InMemory().generateQueueObject(0))
    val ts0 = generateAllStates()
    b.updateTransactionState(ts0(POST))
    b.getState(ts0(POST).transactionID).isDefined shouldBe false
  }

  it should "move to cancel impossible" in {
    val b = new TransactionBuffer(new QueueBuilder.InMemory().generateQueueObject(0))
    val ts0 = generateAllStates()
    b.updateTransactionState(ts0(CANCEL))
    b.getState(ts0(CANCEL).transactionID).isDefined shouldBe false
  }

  it should "move to update impossible" in {
    val b = new TransactionBuffer(new QueueBuilder.InMemory().generateQueueObject(0))
    val ts0 = generateAllStates()
    b.updateTransactionState(ts0(UPDATE))
    b.getState(ts0(UPDATE).transactionID).isDefined shouldBe false
  }

  it should "signal for one completed transaction" in {
    val q = new QueueBuilder.InMemory().generateQueueObject(0)
    val b = new TransactionBuffer(q)
    val ts0 = generateAllStates()
    val ts1 = generateAllStates()
    b.updateTransactionState(ts0(OPENED))
    b.updateTransactionState(ts0(POST))
    b.signalCompleteTransactions()
    val r = q.get(1, TimeUnit.MILLISECONDS)
    r.size shouldBe 1
    r.head.transactionID shouldBe ts0(OPENED).transactionID
    b.getState(ts0(OPENED).transactionID).isDefined shouldBe false
  }


  it should "signal for two completed transactions" in {
    val q = new QueueBuilder.InMemory().generateQueueObject(0)
    val b = new TransactionBuffer(q)
    val ts0 = generateAllStates()
    val ts1 = generateAllStates()
    b.updateTransactionState(ts0(OPENED))
    b.updateTransactionState(ts1(OPENED))
    b.updateTransactionState(ts0(POST))
    b.updateTransactionState(ts1(POST))
    b.signalCompleteTransactions()
    val r = q.get(1, TimeUnit.MILLISECONDS)
    r.size shouldBe 2
    r.head.transactionID shouldBe ts0(OPENED).transactionID
    r.tail.head.transactionID shouldBe ts1(OPENED).transactionID
  }

  it should "signal for first incomplete, second completed" in {
    val q = new QueueBuilder.InMemory().generateQueueObject(0)
    val b = new TransactionBuffer(q)
    val ts0 = generateAllStates()
    val ts1 = generateAllStates()
    b.updateTransactionState(ts0(OPENED))
    b.updateTransactionState(ts1(OPENED))
    b.updateTransactionState(ts1(POST))
    b.signalCompleteTransactions()
    val r = q.get(1, TimeUnit.MILLISECONDS)
    r shouldBe null
  }

  it should "signal for first incomplete, second incomplete" in {
    val q = new QueueBuilder.InMemory().generateQueueObject(0)
    val b = new TransactionBuffer(q)
    val ts0 = generateAllStates()
    val ts1 = generateAllStates()
    b.updateTransactionState(ts0(OPENED))
    b.updateTransactionState(ts1(OPENED))
    b.signalCompleteTransactions()
    val r = q.get(1, TimeUnit.MILLISECONDS)
    r shouldBe null
  }

  it should "signal for first complete, second incomplete" in {
    val q = new QueueBuilder.InMemory().generateQueueObject(0)
    val b = new TransactionBuffer(q)
    val ts0 = generateAllStates()
    val ts1 = generateAllStates()
    b.updateTransactionState(ts0(OPENED))
    b.updateTransactionState(ts1(OPENED))
    b.updateTransactionState(ts0(POST))
    b.signalCompleteTransactions()
    val r = q.get(1, TimeUnit.MILLISECONDS)
    r.size shouldBe 1
  }

  it should "signal for first incomplete, second complete, first complete" in {
    val q = new QueueBuilder.InMemory().generateQueueObject(0)
    val b = new TransactionBuffer(q)
    val ts0 = generateAllStates()
    val ts1 = generateAllStates()
    b.updateTransactionState(ts0(OPENED))
    b.updateTransactionState(ts1(OPENED))
    b.updateTransactionState(ts1(POST))
    b.updateTransactionState(ts0(POST))
    b.signalCompleteTransactions()
    val r = q.get(10, TimeUnit.MILLISECONDS)
    r.size shouldBe 2
    r.head.transactionID shouldBe ts0(OPENED).transactionID
    r.tail.head.transactionID shouldBe ts1(OPENED).transactionID
  }

  it should "handle queue length thresholds properly" in {
    val b = new TransactionBuffer(new QueueBuilder.InMemory().generateQueueObject(0), 100)
    (0 until 100).foreach(_ => {
      val ts0 = generateAllStates()
      b.updateTransactionState(ts0(OPENED))
    })
    b.signalCompleteTransactions()
    b.getSize shouldBe 0
  }
}