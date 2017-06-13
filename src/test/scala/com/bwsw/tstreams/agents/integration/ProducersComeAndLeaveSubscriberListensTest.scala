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

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.agents.producer.NewProducerTransactionPolicy
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer

/**
  * Created by Ivan Kudryavtsev on 13.04.17.
  */
class ProducersComeAndLeaveSubscriberListensTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  lazy val srv = TestStorageServer.getNewClean()

  override def beforeAll(): Unit = {
    srv
    createNewStream()
  }

  it should "handle all transactions and do not loose them" in {
    val TRANSACTIONS_PER_PRODUCER = 100
    val PRODUCERS = 10
    val TRANSACTIONS_PER_SEC = 100 // don't change it
    val producerAccumulator = ListBuffer[Long]()
    val subscriberAccumulator = ListBuffer[Long]()

    var lastID = 0
    var newID = 0
    var counter = 0

    val latch = new CountDownLatch(PRODUCERS * TRANSACTIONS_PER_PRODUCER)

    val subscriber = f.getSubscriber(name = "sv2",
      partitions = Set(0),
      offset = Oldest,
      useLastOffset = false,
      callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => this.synchronized {
        subscriberAccumulator.append(transaction.getTransactionID)
        latch.countDown()
      })

    subscriber.start()

    (0 until PRODUCERS).foreach(_ => {
      val producer = f.getProducer(
        name = "test_producer",
        partitions = Set(0))
      (0 until TRANSACTIONS_PER_PRODUCER).foreach(_ => {
        val transaction = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)
        transaction.send("test")
        transaction.checkpoint()
        producerAccumulator.append(transaction.getTransactionID)
      })
      producer.stop()
      Thread.sleep(100)
      newID = subscriberAccumulator.size

      logger.info(s"Last: $lastID, New: $newID, Counter: $counter")
      lastID = newID
    })
    latch.await(100 + TRANSACTIONS_PER_PRODUCER * PRODUCERS / TRANSACTIONS_PER_SEC, TimeUnit.SECONDS) shouldBe true
    producerAccumulator shouldBe subscriberAccumulator

    subscriber.stop()
  }

  override def afterAll(): Unit = {
    TestStorageServer.dispose(srv)
    onAfterAll()
  }

}
