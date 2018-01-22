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
import com.bwsw.tstreams.env.ConfigurationOptions
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 25.05.17.
  */
class ProducerCancelPartitionTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  private lazy val server = TestStorageServer.getNewClean()

  override def beforeAll(): Unit = {
    server
    createNewStream()
  }

  override def afterAll(): Unit = {
    TestStorageServer.dispose(server)
    onAfterAll()
  }

  "Subscriber" should "cancel transactions for a partition and read next ones without a delay" in {
    val TOTAL_CANCEL = 100
    val TOTAL_CHECKPOINT = 100
    val latch = new CountDownLatch(TOTAL_CHECKPOINT)

    val producer = f.getProducer(
      name = "test_producer",
      partitions = Set(0))

    val subscriber = f.getSubscriber(name = "subscriber",
      partitions = Set(0),
      offset = Newest,
      useLastOffset = false,
      callback = (_: TransactionOperator, _: ConsumerTransaction) => latch.countDown())
    subscriber.start()

    producer.cancel(0) // cancel empty one

    (0 until TOTAL_CANCEL).foreach(_ => producer.newTransaction())
    producer.cancel(0)

    (0 until TOTAL_CHECKPOINT).foreach(_ => producer.newTransaction().send("test"))
    (0 until TOTAL_CHECKPOINT).foreach(_ => producer.newTransaction().send("test"))
    producer.checkpoint(0)

    latch.await(f.getProperty(ConfigurationOptions.Producer.Transaction.ttlMs).asInstanceOf[Int] / 2, TimeUnit.MILLISECONDS) shouldBe true
    producer.stop()
    subscriber.stop()
  }

}
