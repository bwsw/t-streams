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
import com.bwsw.tstreams.agents.consumer.subscriber.Callback
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import com.bwsw.tstreamstransactionserver.exception.Throwable.ServerConnectionException
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 19.05.17.
  */
class StorageBlinkingTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  override def beforeAll(): Unit = {
    val server = TestStorageServer.getNewClean()
    createNewStream()
    TestStorageServer.dispose(server)
  }

  override def afterAll(): Unit = {
    onAfterAll()
  }

  "Producer and subscriber" should "fail when storage blinks" in {
    val subscriberLatch = new CountDownLatch(1)
    val pause = 5000
    val partitions = Set(0)
    val server1 = TestStorageServer.get()

    val producer = f.getProducer(
      name = "producer",
      partitions = partitions)

    val subscriber = f.getSubscriber(name = "subscriber",
      partitions = partitions,
      offset = Oldest,
      useLastOffset = true,
      callback = new Callback {
        override def onTransaction(consumer: TransactionOperator, transaction: ConsumerTransaction): Unit =
          subscriberLatch.countDown()

        override def onFailure(exception: Throwable): Unit =
          exception shouldBe a[ServerConnectionException]
      }).start()

    producer.newTransaction().send("data")
    producer.checkpoint()
    subscriberLatch.await(pause, TimeUnit.MILLISECONDS) shouldBe true

    producer.newTransaction().send("data")

    TestStorageServer.dispose(server1)
    val server2 = TestStorageServer.get()
    Thread.sleep(pause) // wait until server started

    a[ServerConnectionException] shouldBe thrownBy {
      producer.checkpoint()
    }

    subscriber.stop()
    TestStorageServer.dispose(server2)
  }
}
