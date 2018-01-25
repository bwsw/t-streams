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
  * Created by ivan on 13.09.16.
  */
class CheckpointGroupAndSubscriberEventsTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  private lazy val server = TestStorageServer.getNewClean()

  override def beforeAll(): Unit = {
    server
    createNewStream()
  }

  override def afterAll(): Unit = {
    TestStorageServer.dispose(server)
    onAfterAll()
  }

  "CheckpointGroup" should "checkpoint all transactions with CG" in {
    val l = new CountDownLatch(1)
    var transactionsCounter: Int = 0

    val group = f.getCheckpointGroup()

    val producer = f.getProducer(
      name = "test_producer",
      partitions = Set(0))

    group.add(producer)

    val subscriber = f.getSubscriber(name = "ss+2",
      partitions = Set(0),
      offset = Newest,
      useLastOffset = true,
      callback = (_: TransactionOperator, transaction: ConsumerTransaction) => this.synchronized {
        transactionsCounter += 1
        val data = new String(transaction.getAll.head)
        data shouldBe "test"
        if (transactionsCounter == 2) {
          l.countDown()
        }
      })
    subscriber.start()
    val txn1 = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened, 0)
    txn1.send("test".getBytes())
    group.checkpoint()
    val txn2 = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened, 0)
    txn2.send("test".getBytes())
    group.checkpoint()
    l.await(5, TimeUnit.SECONDS) shouldBe true
    transactionsCounter shouldBe 2
    subscriber.stop()
    producer.stop()
  }

}