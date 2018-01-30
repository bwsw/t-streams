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

import com.bwsw.tstreams.agents.consumer.Offset
import com.bwsw.tstreams.agents.producer.{NewProducerTransactionPolicy, Producer, ProducerTransactionImpl}
import com.bwsw.tstreams.env.ConfigurationOptions
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.SingleNodeTestingServer
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates.{Checkpointed, Invalid}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers}

class ProducerTest
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with TestUtils {

  private val partitionsCount = 10
  private val partitions: Set[Int] = (0 until partitionsCount).toSet
  private val waitingTimeout = 5000

  private var server: SingleNodeTestingServer = _
  private var producer: Producer = _

  f.setProperty(ConfigurationOptions.Stream.partitionsCount, partitionsCount)

  override protected def beforeEach(): Unit = {
    server = TestStorageServer.getNewClean()
    createNewStream(partitions = partitionsCount)
    producer = f.getProducer(
      name = "test_producer",
      partitions = partitions)
  }

  override protected def afterEach(): Unit = {
    producer.close()
    TestStorageServer.dispose(server)
  }


  "BasicProducer.newTransaction()" should "return BasicProducerTransaction instance" in {
    val transaction = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)
    transaction.checkpoint()
    transaction.isInstanceOf[ProducerTransactionImpl] shouldEqual true
  }

  "BasicProducer.newTransaction(ProducerPolicies.ErrorIfOpened)" should
    "throw exception if previous transaction was not closed" in {
    val transaction1 = producer.newTransaction(NewProducerTransactionPolicy.CheckpointIfOpened, 2)
    intercept[IllegalStateException] {
      producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened, 2)
    }
    transaction1.checkpoint()
  }

  "BasicProducer.newTransaction(ProducerPolicies.EnqueueIfOpened)" should
    "not throw exception if previous transaction was not closed" in {
    val transaction1 = producer.newTransaction(NewProducerTransactionPolicy.EnqueueIfOpened, 3)
    val transaction2 = producer.newTransaction(NewProducerTransactionPolicy.EnqueueIfOpened, 3)
    transaction1.checkpoint()
    producer.getOpenedTransactionsForPartition(3).get.size shouldBe 1
    transaction2.checkpoint()
    producer.getOpenedTransactionsForPartition(3).get.size shouldBe 0
  }

  "BasicProducer.newTransaction(ProducerPolicies.EnqueueIfOpened) and checkpoint" should
    "not throw exception if previous transaction was not closed" in {
    val transaction1 = producer.newTransaction(NewProducerTransactionPolicy.EnqueueIfOpened, 3)
    val transaction2 = producer.newTransaction(NewProducerTransactionPolicy.EnqueueIfOpened, 3)
    producer.checkpoint()
    transaction1.isClosed shouldBe true
    transaction2.isClosed shouldBe true
  }

  "BasicProducer.newTransaction(CheckpointIfOpen)" should
    "not throw exception if previous transaction was not closed" in {
    producer.newTransaction(NewProducerTransactionPolicy.CheckpointIfOpened, 2)
    val transaction2 = producer.newTransaction(NewProducerTransactionPolicy.CheckpointIfOpened, 2)
    transaction2.checkpoint()
  }

  "BasicProducer.getTransaction()" should
    "return transaction reference if it was created or None" in {
    val transaction = producer.newTransaction(NewProducerTransactionPolicy.CheckpointIfOpened, 1)
    val transactionRef = producer.getOpenedTransactionsForPartition(1)
    transaction.checkpoint()
    transactionRef.get.contains(transaction) shouldBe true
  }

  "BasicProducer.instantTransaction" should "work well for reliable delivery" in {
    val data = Seq(new Array[Byte](128))
    producer.instantTransaction(0, data, isReliable = true) > 0 shouldBe true
  }

  "BasicProducer.instantTransaction" should "work well for unreliable delivery" in {
    val data = Seq(new Array[Byte](128))
    producer.instantTransaction(0, data, isReliable = false) == 0 shouldBe true
  }

  "BasicProducer.instantTransaction" should
    "work and doesn't prevent from correct functioning of regular one" in {
    val regularTransaction = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened, 0)
    regularTransaction.send("test".getBytes)
    val data = Seq(new Array[Byte](128))
    producer.instantTransaction(0, data, isReliable = false) == 0 shouldBe true
    regularTransaction.checkpoint()
  }


  "Producer" should "checkpoint transactions properly" in {
    server.notifyProducerTransactionCompleted(_ => false, println("."))


    val partition = partitions.head
    val consumer = f.getConsumer(
      name = "test_consumer",
      partitions = partitions,
      offset = Offset.Oldest)
    consumer.start()

    val nonEmptyTransaction1 = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened, partition)
    nonEmptyTransaction1.send("data")
    producer.checkpoint()

    val emptyTransaction = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened, partition)
    producer.checkpoint()

    val nonEmptyTransaction2 = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened, partition)
    val latch = new CountDownLatch(1)
    server.notifyProducerTransactionCompleted(
      t => t.transactionID == nonEmptyTransaction2.getTransactionID && t.state == Checkpointed,
      latch.countDown())

    nonEmptyTransaction2.send("data")
    producer.checkpoint()

    latch.await(waitingTimeout, TimeUnit.MILLISECONDS)

    checkTransactions(
      consumer,
      partition,
      Table(
        ("transaction", "state"),
        (nonEmptyTransaction1, Checkpointed),
        (nonEmptyTransaction2, Checkpointed),
        (emptyTransaction, Invalid)))

    consumer.close()
  }


  it should "cancel transactions properly" in {
    val partition = partitions.head
    val consumer = f.getConsumer(
      name = "test_consumer",
      partitions = partitions,
      offset = Offset.Oldest)
    consumer.start()

    val nonEmptyTransaction1 = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened, partition)
    nonEmptyTransaction1.send("data")
    producer.cancel()

    val emptyTransaction = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened, partition)
    producer.cancel()

    val nonEmptyTransaction2 = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened, partition)
    val latch = new CountDownLatch(1)
    server.notifyProducerTransactionCompleted(
      t => t.transactionID == nonEmptyTransaction2.getTransactionID && t.state == Invalid,
      latch.countDown())

    nonEmptyTransaction2.send("data")
    producer.cancel()

    latch.await(waitingTimeout, TimeUnit.MILLISECONDS)

    checkTransactions(
      consumer,
      partition,
      Table(
        ("transaction", "state"),
        (nonEmptyTransaction1, Invalid),
        (nonEmptyTransaction2, Invalid),
        (emptyTransaction, Invalid)))

    consumer.close()
  }


  override protected def afterAll(): Unit = onAfterAll()
}
