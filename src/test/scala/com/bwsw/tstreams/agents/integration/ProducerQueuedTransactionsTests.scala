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
import com.bwsw.tstreams.env.ConfigurationOptions
import com.bwsw.tstreams.env.defaults.TStreamsFactoryProducerDefaults
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer

/**
  * Created by Ivan Kudryavtsev on 16.05.17.
  */
class ProducerQueuedTransactionsTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  lazy val server = TestStorageServer.getNewClean()

  override def beforeAll(): Unit = {
    server
    createNewStream()
  }

  override def afterAll(): Unit = {
    TestStorageServer.dispose(server)
    onAfterAll()
  }

  it should "create queued transactions, checkpoint them in arbitrary order and subscriber must be able to read them" in {
    val testComplete = new CountDownLatch(3)
    val subscriberAccumulator = new ListBuffer[Long]()

    val producer = f.getProducer(name = "test_producer", partitions = Set(0))

    val s = f.getSubscriber(name = "subscriber",
      partitions = Set(0),
      offset = Newest,
      useLastOffset = false,
      callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => this.synchronized {
        subscriberAccumulator.append(transaction.getTransactionID)
        testComplete.countDown()
      }).start()

    val transaction1 = producer.newTransaction(NewProducerTransactionPolicy.EnqueueIfOpened).send("test-t1")
    val transaction2 = producer.newTransaction(NewProducerTransactionPolicy.EnqueueIfOpened).send("test-t2")
    val transaction3 = producer.newTransaction(NewProducerTransactionPolicy.EnqueueIfOpened).send("test-t3")

    Seq(transaction3, transaction1, transaction2).foreach(_.checkpoint())

    producer.checkpoint()
    producer.stop()

    testComplete.await(10, TimeUnit.SECONDS) shouldBe true
    subscriberAccumulator.toSeq shouldBe Seq(transaction1, transaction2, transaction3).map(_.getTransactionID)
    s.stop()
  }


  it should "create queued transactions, write them and subscriber must be able to read them" in {
    val TOTAL = 10000
    val latch = new CountDownLatch(TOTAL)

    val producer = f.getProducer(name = "test_producer", partitions = Set(0))

    val s = f.getSubscriber(name = "subscriber",
      partitions = Set(0),
      offset = Newest,
      useLastOffset = false,
      callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => latch.countDown())
    s.start()

    for (it <- 0 until TOTAL) {
      producer.newTransaction(NewProducerTransactionPolicy.EnqueueIfOpened).send("test")
    }

    producer.checkpoint()
    producer.stop()

    latch.await(60, TimeUnit.SECONDS) shouldBe true

    s.stop()
  }

  it should "create queued transactions, keep them alive, write them and subscriber must be able to read them" in {
    val TOTAL = 1000
    val latch = new CountDownLatch(TOTAL)

    val producer = f.getProducer(
      name = "test_producer",
      partitions = Set(0))

    val subscriber = f.getSubscriber(name = "subscriber",
      partitions = Set(0),
      offset = Newest,
      useLastOffset = false,
      callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => latch.countDown())
    subscriber.start()

    for (it <- 0 until TOTAL) {
      producer.newTransaction(NewProducerTransactionPolicy.EnqueueIfOpened).send("test")
    }

    Thread.sleep(f.getProperty(ConfigurationOptions.Producer.Transaction.ttlMs).asInstanceOf[Int] + 1000)

    producer.checkpoint()
    producer.stop()

    latch.await(60, TimeUnit.SECONDS) shouldBe true

    subscriber.stop()
  }

  it should "create queued transactions, cancel them, create and get correctly with big TTL" in {
    val TOTAL = 1000
    val latch = new CountDownLatch(TOTAL)
    val producerAcc = ListBuffer[Long]()
    val subscriberAcc = ListBuffer[Long]()
    val nf = f.copy()
    nf.setProperty(ConfigurationOptions.Producer.Transaction.ttlMs, TStreamsFactoryProducerDefaults.Producer.Transaction.ttlMs.max)

    val producer = nf.getProducer(
      name = "test_producer",
      partitions = Set(0))

    val s = nf.getSubscriber(name = "subscriber",
      partitions = Set(0),
      offset = Newest,
      useLastOffset = false,
      callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => this.synchronized {
        subscriberAcc.append(transaction.getTransactionID)
        latch.countDown()
      })
    s.start()

    for (it <- 0 until TOTAL) {
      producer.newTransaction(NewProducerTransactionPolicy.EnqueueIfOpened).send("test")
    }

    producer.cancel()

    for (it <- 0 until TOTAL) {
      producerAcc
        .append(producer.newTransaction(NewProducerTransactionPolicy.EnqueueIfOpened)
          .send("test").getTransactionID)
    }
    producer.checkpoint()

    producer.stop()

    latch.await(60, TimeUnit.SECONDS) shouldBe true
    producerAcc shouldBe subscriberAcc

    s.stop()
  }
}
