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

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.producer.NewProducerTransactionPolicy
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 09.09.16.
  */
class ConsumerCheckpointTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  private lazy val server = TestStorageServer.getNewClean()

  override def beforeAll(): Unit = {
    server
    createNewStream()
  }


  "Consumer" should "handle checkpoints correctly" in {

    val CONSUMER_NAME = "test_consumer"

    val producer = f.getProducer(
      name = "test_producer",
      partitions = Set(0))

    val c1 = f.getConsumer(
      name = CONSUMER_NAME,
      partitions = Set(0),
      offset = Oldest,
      useLastOffset = false)

    val c2 = f.getConsumer(
      name = CONSUMER_NAME,
      partitions = Set(0),
      offset = Oldest,
      useLastOffset = true)

    val t1 = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened, 0)
    t1.send("data".getBytes())
    producer.checkpoint()

    val t2 = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened, 0)
    val l2 = new CountDownLatch(1)
    server.notifyProducerTransactionCompleted(t => t.transactionID == t2.getTransactionID && t.state == TransactionStates.Checkpointed, l2.countDown())
    t2.send("data".getBytes())
    producer.checkpoint()
    l2.await()

    c1.start()
    c1.getTransactionById(0, t1.getTransactionID).isDefined shouldBe true
    c1.getTransactionById(0, t2.getTransactionID).isDefined shouldBe true

    val l = new CountDownLatch(1)
    server.notifyConsumerTransactionCompleted(ct => t1.getTransactionID == ct.transactionID, l.countDown())

    c1.getTransaction(0).get.getTransactionID shouldBe t1.getTransactionID
    c1.checkpoint()
    c1.stop()


    l.await()

    c2.start()
    c2.getTransaction(0).get.getTransactionID shouldBe t2.getTransactionID
    c2.checkpoint()
    c2.getTransaction(0).isDefined shouldBe false
    c2.stop()

    producer.stop()
  }

  override def afterAll(): Unit = {
    TestStorageServer.dispose(server)
    onAfterAll()
  }
}
