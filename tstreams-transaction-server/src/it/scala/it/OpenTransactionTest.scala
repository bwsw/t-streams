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
import com.bwsw.tstreamstransactionserver.netty.server.transactionIDService.TransactionIdService
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions
import com.bwsw.tstreamstransactionserver.rpc.{ProducerTransaction, TransactionStates}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import util.Utils
import util.Utils.startZkServerAndGetIt

import scala.concurrent.Await
import scala.concurrent.duration._

class OpenTransactionTest
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll {

  private lazy val (zkServer, zkClient) =
    startZkServerAndGetIt

  private val commitLogToBerkeleyDBTaskDelayMs = 100
  private lazy val serverBuilder = new SingleNodeServerBuilder()
    .withCommitLogOptions(SingleNodeServerOptions.CommitLogOptions(
      closeDelayMs = commitLogToBerkeleyDBTaskDelayMs
    ))

  private lazy val clientBuilder = new ClientBuilder()

  private def getRandomStream = Utils.getRandomStream

  private val secondsWait = 10

  override def beforeAll(): Unit = {
    zkServer
    zkClient
  }

  override def afterAll(): Unit = {
    zkClient.close()
    zkServer.close()
  }

  "Client" should "put producer 'opened' transactions and should get them all" in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { _ =>
      val stream = getRandomStream
      val streamID = Await.result(bundle.client.putStream(stream), secondsWait.seconds)

      val ALL = 80

      val partition = 1

      val from = TransactionIdService.getTransaction()

      val transactions = (0 until ALL).map { _ =>
        bundle.client.openTransaction(streamID, partition, 24000L)
      }

      Thread.sleep(2000)
      val to = TransactionIdService.getTransaction()

      val res = Await.result(bundle.client.scanTransactions(
        streamID, partition, from, to, Int.MaxValue, Set()
      ), secondsWait.seconds)

      res.producerTransactions.size shouldBe transactions.size
      res.producerTransactions.forall(_.state == TransactionStates.Opened) shouldBe true
    }
  }

  it should "open producer transactions and then checkpoint them and should get them all" in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { _ =>

      val stream = getRandomStream
      val streamID = Await.result(bundle.client.putStream(stream), secondsWait.seconds)

      val ALL = 100

      val partition = 1


      val transactionsIDs = (0 until ALL).map { _ =>
        val id = Await.result(
          bundle.client.openTransaction(streamID, partition, 24000L),
          secondsWait.seconds
        )
        id
      }

      val latch1 = new CountDownLatch(1)
      bundle.transactionServer.notifyProducerTransactionCompleted(producerTransaction => {
        producerTransaction.transactionID == transactionsIDs.last &&
          producerTransaction.state == TransactionStates.Checkpointed
      },
        latch1.countDown()
      )


      transactionsIDs.foreach { id =>
        bundle.client.putProducerState(
          ProducerTransaction(streamID, partition, id, TransactionStates.Checkpointed, 0, 25000L)
        )
      }


      latch1.await(secondsWait, TimeUnit.SECONDS)

      val res = Await.result(bundle.client.scanTransactions(
        streamID, partition, transactionsIDs.head, transactionsIDs.last, Int.MaxValue, Set(TransactionStates.Opened)
      ), secondsWait.seconds)


      res.producerTransactions.size shouldBe transactionsIDs.size
      res.producerTransactions.forall(_.state == TransactionStates.Checkpointed) shouldBe true
    }
  }

  it should "put producer 'opened' transaction and get notification of it." in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { _ =>

      val stream = getRandomStream
      val streamID = Await.result(bundle.client.putStream(stream), secondsWait.seconds)

      val latch = new CountDownLatch(1)
      bundle.transactionServer
        .notifyProducerTransactionCompleted(producerTransaction =>
          producerTransaction.state == TransactionStates.Opened, latch.countDown()
        )

      bundle.client.openTransaction(streamID, 1, 25000)

      latch.await(secondsWait, TimeUnit.SECONDS) shouldBe true
    }
  }
}
