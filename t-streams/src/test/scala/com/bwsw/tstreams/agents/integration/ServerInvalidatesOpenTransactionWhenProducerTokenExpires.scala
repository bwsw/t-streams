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

import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, Offset, TransactionOperator}
import com.bwsw.tstreams.agents.producer.NewProducerTransactionPolicy
import com.bwsw.tstreams.env.ConfigurationOptions
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.AuthenticationOptions
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.util.Try

/** Validates that server invalidates producer's open transactions when producer's token expires
  *
  * 1. Create subscriber S
  *
  * 2. Create producer P1, open transaction T1 by P1 (ts1)
  *
  * 3. Create producer P2, open transaction T2 by P2 (ts2), checkpoint T2 (ts3)
  *
  * 4. Kill P1 (ts4), (P1 token expired at ts5)
  *
  * 5. Verify that S got T2 (ts6) before T1.ttl expired (ts7)
  *
  * {{{
  *              |--------------------- T1 ttl -----------------|
  *              |------------------- P1 token ttl -|           |
  * timeline +---|-----|-----|-----|----------------|-----|-----|---->
  *             ts1   ts2   ts3   ts4              ts5   ts6   ts7
  * }}}
  *
  * @author Pavel Tomskikh
  */
class ServerInvalidatesOpenTransactionWhenProducerTokenExpires
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll
    with TestUtils {

  private val partitionsCount = 1
  private val partitions = (0 until partitionsCount).toSet
  private val tokenTtlSec = 5
  private val keepAliveIntervalMs = 1000
  private val keepAliveThreshold = 3
  private val awaitTimeout = 5
  private val factory = f.copy()
  private val authenticationOptions = TestStorageServer.Builder.Defaults.authenticationOptions
    .copy(keyCacheExpirationTimeSec = tokenTtlSec)
  private val serverBuilder = TestStorageServer.Builder(authenticationOptions = authenticationOptions)
  private lazy val server = TestStorageServer.getNewClean(serverBuilder)

  override protected def beforeAll(): Unit = {
    server
    createNewStream(partitions = partitionsCount)
    factory.setProperty(ConfigurationOptions.StorageClient.keepAliveIntervalMs, keepAliveIntervalMs)
    factory.setProperty(ConfigurationOptions.StorageClient.keepAliveThreshold, keepAliveThreshold)
    factory.setProperty(ConfigurationOptions.Stream.partitionsCount, partitionsCount)
    factory.setProperty(ConfigurationOptions.Stream.ttlSec, 60)
    factory.setProperty(ConfigurationOptions.Producer.Transaction.ttlMs, 60 * 1000)
    factory.setProperty(ConfigurationOptions.Producer.Transaction.keepAliveMs, 10 * 1000)
  }

  override protected def afterAll(): Unit = {
    TestStorageServer.dispose(server)
  }


  "Subscriber" should "get second transaction if first transaction's producer is broken and it's token is expired " +
    "but first transaction's ttl isn't expired" in {

    val producer1ThreadGroup = new ThreadGroup("producer1_tg")

    val createTransaction1Latch = new CountDownLatch(1)
    val transaction1OpenedLatch = new CountDownLatch(1)
    new Thread(
      producer1ThreadGroup,
      () => {
        val producer1 = factory.getProducer("test_producer_1", partitions)
        createTransaction1Latch.await()
        val transaction1 = producer1.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)
        transaction1.send("data")
        transaction1OpenedLatch.countDown()
        Thread.sleep(60 * 1000)
      }).start()

    val producer2 = factory.getProducer("test_producer_2", partitions)

    val subscriberLatch = new CountDownLatch(1)
    var consumedTransactionId: Option[Long] = None
    val subscriber = factory.getSubscriber(
      "test_subscriber",
      partitions,
      (_: TransactionOperator, transaction: ConsumerTransaction) => synchronized {
        if (consumedTransactionId.isEmpty) {
          subscriberLatch.countDown()
          consumedTransactionId = Some(transaction.getTransactionID)
        }
      },
      Offset.Newest)

    val testResult = Try {
      subscriber.start()

      createTransaction1Latch.countDown()
      transaction1OpenedLatch.await(awaitTimeout, TimeUnit.SECONDS)

      val transaction2 = producer2.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)
      transaction2.send("data")
      producer2.checkpoint()

      producer1ThreadGroup.interrupt()

      subscriberLatch.await(tokenTtlSec * 2, TimeUnit.SECONDS) shouldBe true
      consumedTransactionId shouldEqual Some(transaction2.getTransactionID)
    }

    subscriber.close()
    producer2.close()

    testResult.get
  }
}
