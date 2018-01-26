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
import com.bwsw.tstreams.env.ConfigurationOptions
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 05.08.16.
  */
class ProducerUpdateTaskTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  private lazy val server = TestStorageServer.getNewClean()

  private lazy val producer = f.getProducer(name = "test_producer", partitions = Set(0, 1, 2))

  override def beforeAll(): Unit = {
    server
    createNewStream()
  }

  "Producer" should "complete in ordered way with delay" in {
    val l = new CountDownLatch(1)

    val consumer = f.getConsumer(name = "test_consumer", partitions = Set(0, 1, 2), offset = Oldest, useLastOffset = true)
      .start()

    val t = producer.newTransaction(policy = NewProducerTransactionPolicy.CheckpointIfOpened, 0)
    val transactionID = t.getTransactionID
    server.notifyProducerTransactionCompleted(t => t.transactionID == transactionID && t.state == TransactionStates.Checkpointed, l.countDown())
    t.send("data".getBytes())
    Thread.sleep(f.getProperty(ConfigurationOptions.Producer.Transaction.ttlMs).asInstanceOf[Int] + 1000)
    t.checkpoint()
    l.await()

    val consumerTransactionOpt = consumer.getTransactionById(0, t.getTransactionID)
    consumerTransactionOpt.isDefined shouldBe true
    consumerTransactionOpt.get.getTransactionID shouldBe t.getTransactionID
    consumerTransactionOpt.get.getCount shouldBe 1
    consumer.stop()
  }

  override def afterAll(): Unit = {
    producer.stop()
    TestStorageServer.dispose(server)
    onAfterAll()
  }
}
