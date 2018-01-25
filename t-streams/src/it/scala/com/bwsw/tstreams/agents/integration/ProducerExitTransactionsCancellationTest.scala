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
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 19.05.17.
  */
class ProducerExitTransactionsCancellationTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  private lazy val server = TestStorageServer.getNewClean()

  override def beforeAll(): Unit = {
    server
    createNewStream()
  }

  override def afterAll(): Unit = {
    TestStorageServer.dispose(server)
    onAfterAll()
  }

  "Subscriber" should "skip fast transactions which are opened but not closed when the producer exits" in {
    val l = new CountDownLatch(2)

    val producer1 = f.getProducer(
      name = "producer1",
      partitions = Set(0))

    val producer2 = f.getProducer(
      name = "producer2",
      partitions = Set(0))

    val s = f.getSubscriber(name = "subscriber",
      partitions = Set(0),
      offset = Newest,
      useLastOffset = false,
      callback = (_: TransactionOperator, _: ConsumerTransaction) => l.countDown())

    s.start()

    producer1
      .newTransaction(policy = NewProducerTransactionPolicy.EnqueueIfOpened, 0)
      .send("")
      .checkpoint()

    producer1.newTransaction(policy = NewProducerTransactionPolicy.EnqueueIfOpened, 0)
    producer1.stop()

    producer2
      .newTransaction(policy = NewProducerTransactionPolicy.EnqueueIfOpened, 0)
      .send("")
      .checkpoint()

    producer2.stop()

    val delay = f.getProperty(ConfigurationOptions.Producer.Transaction.ttlMs).asInstanceOf[Int] / 2
    l.await(delay, TimeUnit.MILLISECONDS) shouldBe true

    s.stop()

  }
}
