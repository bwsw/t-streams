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
import com.bwsw.tstreams.agents.producer.{NewProducerTransactionPolicy, ProducerTransactionImpl}
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer

class ProducerToSubscriberStartsAfterWriteWithCheckpointGroupTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  val COUNT = 10

  private lazy val server = TestStorageServer.getNewClean()

  override def beforeAll(): Unit = {
    server
    createNewStream()
  }

  "Clients" should s"The producer sends $COUNT transactions, subscriber receives $COUNT of them when started after." +
    s"Then do group checkpoint and start new Subscriber from checkpointed place" in {
    val group = f.getCheckpointGroup()

    val bp = ListBuffer[Long]()
    val bs = ListBuffer[Long]()

    val subscriber1Latch = new CountDownLatch(1)

    val producer = f.getProducer(
      name = "test_producer1",
      partitions = Set(0))

    val subscriber = f.getSubscriber(name = "ss+2",
      partitions = Set(0),
      offset = Oldest,
      useLastOffset = true,
      callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => this.synchronized {
        bs.append(transaction.getTransactionID)
        consumer.setStreamPartitionOffset(transaction.getPartition, transaction.getTransactionID)
        if (bs.size == COUNT) {
          subscriber1Latch.countDown()
        }
      })

    for (_ <- 0 until COUNT) {
      val t: ProducerTransactionImpl = producer.newTransaction(policy = NewProducerTransactionPolicy.CheckpointIfOpened)
      t.send("test")
      t.checkpoint()
      bp.append(t.getTransactionID)
    }

    producer.stop()

    val lastTxn = bp.max
    val ttsSynchronizationLatch = new CountDownLatch(1)
    server.notifyConsumerTransactionCompleted(ct => lastTxn == ct.transactionID, ttsSynchronizationLatch.countDown())

    group.add(subscriber)
    subscriber.start()
    subscriber1Latch.await(10, TimeUnit.SECONDS) shouldBe true
    group.checkpoint()
    subscriber.stop()
    bs.size shouldBe COUNT

    ttsSynchronizationLatch.await(10, TimeUnit.SECONDS) shouldBe true

    val subscriber2Latch = new CountDownLatch(1)

    val s2 = f.getSubscriber(name = "ss+2",
      partitions = Set(0),
      offset = Oldest,
      useLastOffset = true,
      callback = (_: TransactionOperator, _: ConsumerTransaction) => this.synchronized {
        subscriber2Latch.countDown()
      })
    s2.start()
    subscriber2Latch.await(5, TimeUnit.SECONDS) shouldBe false
    s2.stop()
  }

  override def afterAll(): Unit = {
    TestStorageServer.dispose(server)
    onAfterAll()
  }
}
