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

import java.util.concurrent.CountDownLatch

import com.bwsw.tstreams.agents.consumer.ConsumerTransaction
import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.producer.NewProducerTransactionPolicy
import com.bwsw.tstreams.common.TimeTracker
import com.bwsw.tstreams.env.ConfigurationOptions
import com.bwsw.tstreams.testutils._
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}


class ProducerAndConsumerCheckpointTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  private lazy val srv = TestStorageServer.getNewClean()

  private lazy val producer = f.getProducer(
    name = "test_producer",
    partitions = Set(0))

  private lazy val consumer = f.getConsumer(
    name = "test_consumer",
    partitions = Set(0),
    offset = Oldest,
    useLastOffset = true)

  private lazy val consumer2 = f.getConsumer(
    name = "test_consumer",
    partitions = Set(0),
    offset = Oldest,
    useLastOffset = true)

  override def beforeAll(): Unit = {
    srv

    createNewStream()
  }

  "Producer and consumer" should "producer - generate many transactions, consumer - retrieve all of them with " +
    "reinitialization after some time" in {
    val TRANSACTIONS_COUNT = 1000
    val dataToSend = (for (_ <- 0 until 1) yield randomKeyspace).sorted

    var counter = 0

    val l = new CountDownLatch(1)

    (0 until TRANSACTIONS_COUNT) foreach { _ =>
      TimeTracker.update_start("Overall")
      TimeTracker.update_start("newTransaction")
      val transaction = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)

      counter += 1
      if (counter == TRANSACTIONS_COUNT)
        srv.notifyProducerTransactionCompleted(t => t.transactionID == transaction.getTransactionID && t.state == TransactionStates.Checkpointed, l.countDown())

      TimeTracker.update_end("newTransaction")

      TimeTracker.update_start("Send")
      dataToSend foreach { part => transaction.send(part.getBytes()) }
      TimeTracker.update_end("Send")

      TimeTracker.update_start("checkpoint")
      transaction.checkpoint()
      TimeTracker.update_end("checkpoint")
      TimeTracker.update_end("Overall")
    }

    val firstPart = TRANSACTIONS_COUNT / 3
    val secondPart = TRANSACTIONS_COUNT - firstPart

    l.await()

    val l1 = new CountDownLatch(1)

    consumer.start()
    (0 until firstPart) foreach { counter =>
      val transaction: ConsumerTransaction = consumer.getTransaction(0).get

      if (counter == firstPart - 1)
        srv.notifyConsumerTransactionCompleted(ct => transaction.getTransactionID == ct.transactionID, l1.countDown())

      transaction.getAll.map(i => new String(i)).sorted shouldBe dataToSend
      consumer.checkpoint()
    }

    l1.await()

    consumer2.start()
    (0 until secondPart) foreach { _ =>
      val transaction: ConsumerTransaction = consumer2.getTransaction(0).get
      transaction.getAll.map(i => new String(i)).sorted shouldBe dataToSend
    }

    //assert that is nothing to read
    (0 until Integer.parseInt(f.getProperty(ConfigurationOptions.Stream.partitionsCount).toString)) foreach { _ =>
      consumer2.getTransaction(0).isEmpty shouldBe true
    }
  }

  override def afterAll(): Unit = {
    producer.stop()
    Seq(consumer, consumer2).foreach(c => c.stop())
    onAfterAll()
    TestStorageServer.dispose(srv)
    TimeTracker.dump()
  }
}