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

package com.bwsw.tstreams.agents.integration

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.consumer.Offset.Newest
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.agents.producer.NewProducerTransactionPolicy
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 26.08.16.
  */
class SubscriberBasicPubSubTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  lazy val srv = TestStorageServer.getNewClean()

  override def beforeAll(): Unit = {
    srv
    createNewStream()
  }

  def testCase(isReliable: Boolean) {
    val TOTAL = 1000
    val latch = new CountDownLatch(TOTAL)

    val producer = f.getProducer(
      name = "test_producer",
      partitions = Set(0))

    val s = f.getSubscriber(name = "sv2_instant",
      partitions = Set(0),
      offset = Newest,
      useLastOffset = false,
      callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => latch.countDown())

    s.start()
    for (it <- 0 until TOTAL) {
      producer.instantTransaction(0, Seq("data".getBytes), isReliable = isReliable)
    }
    producer.stop()
    latch.await(60, TimeUnit.SECONDS) shouldBe true
    s.stop()
  }

  it should "handle all transactions producer by producer with instant transactions (reliable)" in {
    testCase(isReliable = true)
  }

  it should "handle all transactions producer by producer with instant transactions (unreliable)" in {
    testCase(isReliable = false)
  }


  it should "handle all transactions produced by producer" in {

    val TOTAL = 1000
    val latch = new CountDownLatch(1)

    var subscriberTransactionsAmount = 0
    val producer = f.getProducer(
      name = "test_producer",
      partitions = Set(0, 1, 2))

    val s = f.getSubscriber(name = "sv2",
      partitions = Set(0, 1, 2),
      offset = Newest,
      useLastOffset = false,
      callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => this.synchronized {
        subscriberTransactionsAmount += 1
        transaction.getAll
        if (subscriberTransactionsAmount == TOTAL)
          latch.countDown()
      })
    s.start()
    for (it <- 0 until TOTAL) {
      val transaction = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)
      transaction.send("test")
      transaction.checkpoint()
    }
    producer.stop()
    latch.await(60, TimeUnit.SECONDS) shouldBe true
    s.stop()
    subscriberTransactionsAmount shouldBe TOTAL
  }

  it should "handle all transactions produced by two different producers" in {

    val TOTAL = 1000
    var subscriberTransactionsAmount = 0
    val latch = new CountDownLatch(1)

    val producer1 = f.getProducer(
      name = "test_producer",
      partitions = Set(0, 1, 2))

    val s = f.getSubscriber(name = "sv2",
      partitions = Set(0, 1, 2),
      offset = Newest,
      useLastOffset = false,
      callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => this.synchronized {
        subscriberTransactionsAmount += 1
        if (subscriberTransactionsAmount == TOTAL * 2)
          latch.countDown()
      })
    s.start()
    for (it <- 0 until TOTAL) {
      val transaction = producer1.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)
      transaction.send("test")
      transaction.checkpoint()
    }
    producer1.stop()
    val producer2 = f.getProducer(
      name = "test_producer2",
      partitions = Set(0, 1, 2))

    for (it <- 0 until TOTAL) {
      val transaction = producer2.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)
      transaction.send("test")
      transaction.checkpoint()
    }
    producer2.stop()
    latch.await(10, TimeUnit.SECONDS) shouldBe true
    s.stop()
    subscriberTransactionsAmount shouldBe TOTAL * 2
  }

  override def afterAll(): Unit = {
    TestStorageServer.dispose(srv)
    onAfterAll()
  }
}
