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

package com.bwsw.tstreamstransactionserver.netty.server.multiNode

import java.util.UUID
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreamstransactionserver.exception.Throwable.{MasterChangedException, MasterLostException}
import com.bwsw.tstreamstransactionserver.netty.client.ClientBuilder
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions.BookkeeperOptions
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.AuthenticationOptions
import com.bwsw.tstreamstransactionserver.rpc.{TransactionInfo, TransactionStates}
import com.bwsw.tstreamstransactionserver.util.Implicit.ProducerTransactionSortable
import com.bwsw.tstreamstransactionserver.util.Utils._
import com.bwsw.tstreamstransactionserver.util.Utils.{getRandomConsumerTransaction, getRandomProducerTransaction, getRandomStream}
import com.bwsw.tstreamstransactionserver.util.multiNode.MultiNodeUtils
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Random, Try}

class CommonCheckpointGroupServerTest
  extends FlatSpec
    with BeforeAndAfterAll
    with Matchers {

  private val ensembleNumber = 3
  private val writeQuorumNumber = 3
  private val ackQuorumNumber = 2

  private val bookkeeperOptions =
    BookkeeperOptions(
      ensembleNumber,
      writeQuorumNumber,
      ackQuorumNumber,
      "test".getBytes()
    )

  private val maxIdleTimeBetweenRecordsMs = 1000
  private lazy val serverBuilder = new CommonCheckpointGroupServerBuilder()
    .withAuthenticationOptions(AuthenticationOptions(tokenTtlSec = 60))
  private lazy val clientBuilder = new ClientBuilder()


  private val bookiesNumber =
    ensembleNumber max writeQuorumNumber max ackQuorumNumber


  private lazy val (zkServer, zkClient, bookieServers) =
    startZkServerBookieServerZkClient(bookiesNumber)

  override def beforeAll(): Unit = {
    zkServer
    zkClient
    bookieServers
  }

  override def afterAll(): Unit = {
    bookieServers.foreach(_.shutdown())
    zkClient.close()
    zkServer.close()
  }


  val secondsWait = 15

  "CommonCheckpointGroupServer" should "[scanTransactions] put transactions and get them back" in {
    val bundle = MultiNodeUtils.getCommonCheckpointGroupServerBundle(
      zkClient, bookkeeperOptions, serverBuilder, clientBuilder, maxIdleTimeBetweenRecordsMs
    )

    bundle.operate { transactionServer =>
      val client = bundle.client
      val stream = getRandomStream
      val streamID = Await.result(client.putStream(stream), secondsWait.seconds)


      val producerTransactions = Array.fill(30)(getRandomProducerTransaction(streamID, stream)).filter(_.state == TransactionStates.Opened) :+
        getRandomProducerTransaction(streamID, stream).copy(state = TransactionStates.Opened)

      val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(streamID, stream))

      val statesAllowed = Array(TransactionStates.Opened, TransactionStates.Updated)
      val (from, to) = (
        producerTransactions.filter(txn => statesAllowed.contains(txn.state)).minBy(_.transactionID).transactionID,
        producerTransactions.filter(txn => statesAllowed.contains(txn.state)).maxBy(_.transactionID).transactionID
      )

      val latch = new CountDownLatch(1)
      transactionServer.notifyProducerTransactionCompleted(
        txn => txn.transactionID == to,
        latch.countDown()
      )

      Await.result(client.putTransactions(producerTransactions, consumerTransactions), secondsWait.seconds)


      latch.await(secondsWait, TimeUnit.SECONDS) shouldBe true

      val resFrom_1From = Await.result(client.scanTransactions(streamID, stream.partitions, from - 1, from, Int.MaxValue, Set()), secondsWait.seconds)
      resFrom_1From.producerTransactions.size shouldBe 1
      resFrom_1From.producerTransactions.head.transactionID shouldBe from


      val resFromFrom = Await.result(client.scanTransactions(streamID, stream.partitions, from, from, Int.MaxValue, Set()), secondsWait.seconds)
      resFromFrom.producerTransactions.size shouldBe 1
      resFromFrom.producerTransactions.head.transactionID shouldBe from


      val resToFrom = Await.result(client.scanTransactions(streamID, stream.partitions, to, from, Int.MaxValue, Set()), secondsWait.seconds)
      resToFrom.producerTransactions.size shouldBe 0

      val producerTransactionsByState = producerTransactions.groupBy(_.state)
      val res = Await.result(client.scanTransactions(streamID, stream.partitions, from, to, Int.MaxValue, Set()), secondsWait.seconds).producerTransactions

      val producerOpenedTransactions = producerTransactionsByState(TransactionStates.Opened).sortBy(_.transactionID)

      res.head shouldBe producerOpenedTransactions.head
      res shouldBe sorted
    }
  }

  it should "put producer and consumer transactions" in {
    val bundle = MultiNodeUtils.getCommonCheckpointGroupServerBundle(
      zkClient, bookkeeperOptions, serverBuilder, clientBuilder, maxIdleTimeBetweenRecordsMs
    )

    bundle.operate { _ =>
      val client = bundle.client

      val stream = getRandomStream
      val streamID = Await.result(client.putStream(stream), secondsWait.seconds)

      val producerTransactions = Array.fill(100)(getRandomProducerTransaction(streamID, stream))
      val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(streamID, stream))

      val result = client.putTransactions(producerTransactions, consumerTransactions)

      Await.result(result, 5.seconds) shouldBe true
    }
  }

  it should "put any kind of binary data and get it back" in {
    val bundle = MultiNodeUtils.getCommonCheckpointGroupServerBundle(
      zkClient, bookkeeperOptions, serverBuilder, clientBuilder, maxIdleTimeBetweenRecordsMs
    )

    bundle.operate { _ =>
      val client = bundle.client


      val stream = getRandomStream
      val streamID = Await.result(client.putStream(stream), secondsWait.seconds)
      streamID shouldNot be(-1)

      val txn = getRandomProducerTransaction(streamID, stream)
      Await.result(client.putProducerState(txn), secondsWait.seconds)

      val dataAmount = 5000
      val data = Array.fill(dataAmount)(Random.nextString(10).getBytes)

      val resultInFuture = Await.result(client.putTransactionData(streamID, txn.partition, txn.transactionID, data, 0), secondsWait.seconds)
      resultInFuture shouldBe true

      val currentOffset = Await.result(client.getCommitLogOffsets(), secondsWait.seconds)
      var isNotOffsetOvercome = true
      while (isNotOffsetOvercome) {
        TimeUnit.MILLISECONDS.sleep(maxIdleTimeBetweenRecordsMs)
        val res =
          Await.result(client.getCommitLogOffsets(), secondsWait.seconds)

        isNotOffsetOvercome =
          currentOffset.currentConstructedCommitLog + 1 > res.currentProcessedCommitLog
      }

      val dataFromDatabase = Await.result(client.getTransactionData(streamID, txn.partition, txn.transactionID, 0, dataAmount), secondsWait.seconds)
      data should contain theSameElementsAs dataFromDatabase
    }
  }

  it should "[putProducerStateWithData] put a producer transaction (Opened) with data, and server should persist data." in {
    val bundle = MultiNodeUtils.getCommonCheckpointGroupServerBundle(
      zkClient, bookkeeperOptions, serverBuilder, clientBuilder, maxIdleTimeBetweenRecordsMs
    )

    bundle.operate { transactionServer =>
      val client = bundle.client

      //arrange
      val stream =
        getRandomStream

      val streamID =
        Await.result(client.putStream(stream), secondsWait.seconds)

      val openedProducerTransaction =
        getRandomProducerTransaction(streamID, stream, TransactionStates.Opened)

      val dataAmount = 30
      val data = Array.fill(dataAmount)(Random.nextString(10).getBytes)

      val latch = new CountDownLatch(1)
      transactionServer.notifyProducerTransactionCompleted(
        txn => txn.transactionID == openedProducerTransaction.transactionID,
        latch.countDown()
      )


      val from = dataAmount
      val to = 2 * from
      Await.result(client.putProducerStateWithData(openedProducerTransaction, data, from), secondsWait.seconds)

      latch.await(secondsWait, TimeUnit.SECONDS) shouldBe true

      val successResponse = Await.result(
        client.getTransaction(streamID, stream.partitions, openedProducerTransaction.transactionID
        ), secondsWait.seconds)

      val successResponseData = Await.result(
        client.getTransactionData(
          streamID, stream.partitions, openedProducerTransaction.transactionID, from, to
        ), secondsWait.seconds)


      //assert
      successResponse shouldBe TransactionInfo(
        exists = true,
        Some(openedProducerTransaction)
      )

      successResponseData should contain theSameElementsInOrderAs data
    }
  }

  "Client" should "throw MasterChangedException when a master changed" in {
    val bundle1 = MultiNodeUtils.getCommonCheckpointGroupServerBundle(
      zkClient, bookkeeperOptions, serverBuilder, clientBuilder, maxIdleTimeBetweenRecordsMs
    )

    bundle1.operate { server1 =>
      val client = bundle1.client

      val storageOptions = serverBuilder.getStorageOptions.copy(path = s"/tmp/tts-${UUID.randomUUID().toString}")
      val serverBuilder2 = serverBuilder.withServerStorageOptions(storageOptions)

      val bundle2 = MultiNodeUtils.getCommonCheckpointGroupServerBundle(
        zkClient, bookkeeperOptions, serverBuilder2, clientBuilder, maxIdleTimeBetweenRecordsMs)

      bundle2.operate { _ =>

        val stream = getRandomStream
        val streamID = Await.result(client.putStream(stream), secondsWait.seconds)

        val producerTransactions = Array.fill(100)(getRandomProducerTransaction(streamID, stream))
        val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(streamID, stream))

        val result = client.putTransactions(producerTransactions, consumerTransactions)

        Await.result(result, 5.seconds) shouldBe true

        server1.shutdown()
        Thread.sleep(1000) // wait until master node in zookeeper updated

        val otherProducerTransactions = Array.fill(100)(getRandomProducerTransaction(streamID, stream))
        val otherConsumerTransactions = Array.fill(100)(getRandomConsumerTransaction(streamID, stream))

        /*
         * The type of this exception can be MasterChangedException if the second server had updated
         * a master node in ZooKeeper before the client received a notification that this node changed,
         * or MasterLostException otherwise
         */
        Try(Await.result(
          client.putTransactions(otherProducerTransactions, otherConsumerTransactions),
          secondsWait.seconds)) should matchPattern {
          case Failure(_: MasterChangedException) =>
          case Failure(_: MasterLostException) =>
        }
      }
    }
  }

  it should "disconnect from server when it is off" in {
    val bundle = MultiNodeUtils.getCommonCheckpointGroupServerBundle(
      zkClient, bookkeeperOptions, serverBuilder, clientBuilder, maxIdleTimeBetweenRecordsMs)

    bundle.operate { server =>
      val client = bundle.client

      Thread.sleep(1000) // wait until client connected from server
      client.isConnected shouldBe true

      server.shutdown()
      Thread.sleep(1000) // wait until client disconnected from server

      client.isConnected shouldBe false
    }
  }

  it should "disconnect from server when master is changed" in {
    val bundle1 = MultiNodeUtils.getCommonCheckpointGroupServerBundle(
      zkClient, bookkeeperOptions, serverBuilder, clientBuilder, maxIdleTimeBetweenRecordsMs)

    bundle1.operate { server1 =>
      val client = bundle1.client

      val storageOptions = serverBuilder.getStorageOptions.copy(path = s"/tmp/tts-${UUID.randomUUID().toString}")
      val serverBuilder2 = serverBuilder.withServerStorageOptions(storageOptions)

      val bundle2 = MultiNodeUtils.getCommonCheckpointGroupServerBundle(
        zkClient, bookkeeperOptions, serverBuilder2, clientBuilder, maxIdleTimeBetweenRecordsMs)

      bundle2.operate { _ =>
        Thread.sleep(1000) // wait until client connected from server
        client.isConnected shouldBe true

        server1.shutdown()
        Thread.sleep(1000) // wait until client disconnected from server

        client.isConnected shouldBe false
      }
    }
  }
}
