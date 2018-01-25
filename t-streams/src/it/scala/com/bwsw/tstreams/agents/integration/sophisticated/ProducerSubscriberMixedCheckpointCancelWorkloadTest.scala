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

package com.bwsw.tstreams.agents.integration.sophisticated

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * Created by Ivan Kudryavtsev on 14.04.17.
  */
class ProducerSubscriberMixedCheckpointCancelWorkloadTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  val TRANSACTION_COUNT = 10000
  val CANCEL_PROBABILITY = 0.05

  override def beforeAll(): Unit = {
  }

  def test(startBeforeProducerSend: Boolean = false): Unit = {
    val producerAccumulator = ListBuffer[Long]()
    val subscriberAccumulator = ListBuffer[Long]()
    val subscriberLatch = new CountDownLatch(1)

    val srv = TestStorageServer.getNewClean()
    createNewStream()

    val subscriber = f.getSubscriber(name = "sv2",
      partitions = Set(0),
      offset = Oldest,
      useLastOffset = false,
      callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => this.synchronized {
        subscriberAccumulator.append(transaction.getTransactionID)
        if (producerAccumulator.size == subscriberAccumulator.size)
          subscriberLatch.countDown()
      })

    val producer = f.getProducer(
      name = "test_producer",
      partitions = Set(0))

    if (startBeforeProducerSend)
      subscriber.start()

    (0 until TRANSACTION_COUNT).foreach(_ => {
      val txn = producer.newTransaction()
      if (CANCEL_PROBABILITY < Random.nextDouble()) {
        txn.send("test".getBytes())
        txn.checkpoint()
        producerAccumulator.append(txn.getTransactionID)
      } else {
        txn.cancel()
      }
    })
    producer.stop()

    if (!startBeforeProducerSend)
      subscriber.start()

    subscriberLatch.await(100, TimeUnit.SECONDS) shouldBe true

    producerAccumulator shouldBe subscriberAccumulator

    subscriber.stop()

    TestStorageServer.dispose(srv)

  }

  it should "accumulate only checkpointed transactions, subscriber starts after" in {
    test(startBeforeProducerSend = false)
  }

  it should "accumulate only checkpointed transactions, subscriber starts before" in {
    test(startBeforeProducerSend = true)
  }

  override def afterAll(): Unit = {
    onAfterAll()
  }
}
