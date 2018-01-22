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

package it


import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreamstransactionserver.netty.client.ClientBuilder
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.SingleNodeServerBuilder
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions
import com.bwsw.tstreamstransactionserver.rpc.ConsumerTransaction
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import util.Utils
import util.Utils.{getRandomStream, startZkServerAndGetIt}

import scala.concurrent.Await
import scala.concurrent.duration._

class SingleNodeServerConsumerTransactionNotificationTest
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll {

  private val commitLogToBerkeleyDBTaskDelayMs = 100
  private lazy val serverBuilder = new SingleNodeServerBuilder()
    .withCommitLogOptions(SingleNodeServerOptions.CommitLogOptions(
      closeDelayMs = commitLogToBerkeleyDBTaskDelayMs
    ))

  private lazy val (zkServer, zkClient) =
    startZkServerAndGetIt

  private lazy val clientBuilder = new ClientBuilder()

  override def beforeAll(): Unit = {
    zkServer
    zkClient
  }

  override def afterAll(): Unit = {
    zkClient.close()
    zkServer.close()
  }

  private val secondsWait = 10

  "Client" should "put consumerCheckpoint and get a transaction id back." in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { transactionServer =>
      val client = bundle.client

      val stream = getRandomStream
      val streamID = Await.result(client.putStream(stream), secondsWait.seconds)

      val transactionId = 10L
      val checkpointName = "test-name"


      val latch = new CountDownLatch(1)
      transactionServer.notifyConsumerTransactionCompleted(consumerTransaction =>
        consumerTransaction.transactionID == transactionId && consumerTransaction.name == checkpointName, latch.countDown()
      )

      val consumerTransactionOuter = ConsumerTransaction(streamID, 1, transactionId, checkpointName)
      client.putConsumerCheckpoint(consumerTransactionOuter)

      latch.await(1, TimeUnit.SECONDS) shouldBe true
    }
  }

  it should "shouldn't get notification." in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { transactionServer =>
      val client = bundle.client
      val stream = getRandomStream
      Await.result(client.putStream(stream), secondsWait.seconds)

      val transactionId = 10L
      val checkpointName = "test-name"

      val latch = new CountDownLatch(1)
      val id = transactionServer.notifyConsumerTransactionCompleted(consumerTransaction =>
        consumerTransaction.transactionID == transactionId && consumerTransaction.name == checkpointName, latch.countDown()
      )

      latch.await(1, TimeUnit.SECONDS) shouldBe false
      transactionServer.removeConsumerNotification(id) shouldBe true
    }
  }

  it should "get notification about consumer checkpoint after using putTransactions method." in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { transactionServer =>
      val client = bundle.client
      val stream = getRandomStream
      val streamID = Await.result(client.putStream(stream), secondsWait.seconds)

      val transactionId = 10L
      val checkpointName = "test-name"


      val latch = new CountDownLatch(1)
      transactionServer.notifyConsumerTransactionCompleted(consumerTransaction =>
        consumerTransaction.transactionID == transactionId, latch.countDown()
      )

      val consumerTransactionOuter = ConsumerTransaction(streamID, 1, transactionId, checkpointName)
      client.putTransactions(Seq(), Seq(consumerTransactionOuter))

      latch.await(15, TimeUnit.SECONDS) shouldBe true
    }
  }

}
