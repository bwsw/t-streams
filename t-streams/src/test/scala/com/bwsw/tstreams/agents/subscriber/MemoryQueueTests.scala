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

import com.bwsw.tstreams.common.MemoryQueue
import com.bwsw.tstreams.IncreasingGenerator
import com.bwsw.tstreamstransactionserver.rpc.{TransactionState, TransactionStates}
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 19.08.16.
  */
class MemoryQueueTests extends FlatSpec with Matchers {

  val authKey = "auth-key"

  it should "created" in {
    val queue = new MemoryQueue[List[TransactionState]]()
  }

  it should "allow to put/get list" in {
    val queue = new MemoryQueue[List[TransactionState]]()
    val state = TransactionState(
      transactionID = IncreasingGenerator.get,
      partition = 0,
      masterID = 1,
      orderID = 1,
      count = 1,
      status = TransactionStates.Opened,
      ttlMs = 1,
      authKey = authKey)
    queue.put(List(state))
    val receivedStates = queue.get(1, TimeUnit.SECONDS)
    receivedStates.size shouldBe 1
    receivedStates.head.transactionID shouldBe state.transactionID
  }

  it should "return null no data in list" in {
    val queue = new MemoryQueue[List[TransactionState]]()
    val receivedStates = queue.get(1, TimeUnit.SECONDS)
    receivedStates shouldBe null
  }

  it should "lock if no data in list" in {
    val queue = new MemoryQueue[List[TransactionState]]()
    val start = System.currentTimeMillis()
    queue.get(10, TimeUnit.MILLISECONDS)
    val end = System.currentTimeMillis()
    end - start > 9 shouldBe true
  }

  it should "work with empty lists" in {
    val queue = new MemoryQueue[List[TransactionState]]()
    queue.put(Nil)
    val receivedStates = queue.get(10, TimeUnit.MILLISECONDS)
    receivedStates shouldBe Nil
  }

  it should "keep all items" in {
    val queue = new MemoryQueue[List[TransactionState]]()
    val emptyStatesAmount = 1000
    for (i <- 0 until emptyStatesAmount) {
      queue.put(Nil)
    }
    var counter = 0
    var receivedStates = queue.get(1, TimeUnit.MILLISECONDS)
    while (receivedStates != null) {
      counter += 1
      receivedStates = queue.get(1, TimeUnit.MILLISECONDS)
    }
    counter shouldBe emptyStatesAmount
  }

  it should "correctly work with inFlight count" in {
    val queue = new MemoryQueue[List[TransactionState]]()
    val emptyStatesAmount = 1000
    for (i <- 0 until emptyStatesAmount) {
      queue.put(Nil)
    }
    queue.getInFlight shouldBe 1000
    for (i <- 0 until emptyStatesAmount) {
      queue.get(1, TimeUnit.MILLISECONDS)
    }
    queue.getInFlight shouldBe 0

    queue.get(1, TimeUnit.MILLISECONDS)
    queue.getInFlight shouldBe 0

  }

  it should "be signalled" in {
    val queue = new MemoryQueue[List[TransactionState]]()
    val job = new Thread(() => {
      Thread.sleep(10)
      queue.put(Nil)
    })
    job.start()
    val start = System.currentTimeMillis()
    queue.get(100, TimeUnit.MILLISECONDS)
    val end = System.currentTimeMillis()
    end - start < 50 shouldBe true
  }
}
