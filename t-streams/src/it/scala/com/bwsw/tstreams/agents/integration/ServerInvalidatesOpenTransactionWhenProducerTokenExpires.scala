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
  private val transactionTtlMs = 60 * 1000
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
    factory.setProperty(ConfigurationOptions.Producer.Transaction.ttlMs, transactionTtlMs)
    factory.setProperty(ConfigurationOptions.Producer.Transaction.keepAliveMs, 10 * 1000)
  }

  override protected def afterAll(): Unit = {
    TestStorageServer.dispose(server)
  }


  "Subscriber" should "get second transaction if first transaction's producer is broken and its token is expired " +
    "but first transaction's ttl isn't expired" in {

    val firstProducerThreadGroup = new ThreadGroup("producer_1_thread_group")

    val createFirstTransactionLatch = new CountDownLatch(1)
    val firstTransactionOpenedLatch = new CountDownLatch(1)
    val testDoneLatch = new CountDownLatch(1)
    val firstProducerThread = new Thread(
      firstProducerThreadGroup,
      () => {
        val firstProducer = factory.getProducer("test_producer_1", partitions)
        createFirstTransactionLatch.await()
        val firstTransaction = firstProducer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened) // ts1

        firstTransaction.send("data")
        firstTransactionOpenedLatch.countDown()
        testDoneLatch.await(transactionTtlMs, TimeUnit.MILLISECONDS)
        firstProducer.close()
      })

    firstProducerThread.start()

    val secondProducer = factory.getProducer("test_producer_2", partitions)

    val subscriberLatch = new CountDownLatch(1)
    var consumedTransactionId: Option[Long] = None
    var consumedTransactionTime: Long = 0 // ts6
    val subscriber = factory.getSubscriber(
      "test_subscriber",
      partitions,
      (_: TransactionOperator, transaction: ConsumerTransaction) => synchronized {
        if (consumedTransactionId.isEmpty) {
          subscriberLatch.countDown()
          consumedTransactionId = Some(transaction.getTransactionID)
          consumedTransactionTime = System.currentTimeMillis()
        }
      },
      Offset.Newest)

    val testResult = Try {
      subscriber.start()

      createFirstTransactionLatch.countDown()
      firstTransactionOpenedLatch.await(awaitTimeout, TimeUnit.SECONDS) // ts1
      val firstTransactionExpirationTime = System.currentTimeMillis() + transactionTtlMs // ts7

      val secondTransaction = secondProducer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened) // ts2
      secondTransaction.send("data")
      secondProducer.checkpoint() // ts3

      firstProducerThreadGroup.interrupt() //ts4

      subscriberLatch.await(tokenTtlSec * 2, TimeUnit.SECONDS) shouldBe true // ts5
      consumedTransactionId shouldEqual Some(secondTransaction.getTransactionID)
      consumedTransactionTime should be > 0L
      consumedTransactionTime should be < firstTransactionExpirationTime
    }

    testDoneLatch.countDown()
    firstProducerThread.join()

    subscriber.close()
    secondProducer.close()

    testResult.get
  }
}
