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

package com.bwsw.tstreamstransactionserver.netty.server.singleNode

import com.bwsw.tstreamstransactionserver.netty.client.ClientBuilder
import com.bwsw.tstreamstransactionserver.options._
import com.bwsw.tstreamstransactionserver.rpc._
import com.bwsw.tstreamstransactionserver.util.Utils._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class StreamOperationsTest
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll {

  private val responseTimeout = 10.seconds
  private val serverBuilder = new SingleNodeServerBuilder()
    .withCommitLogOptions(
      SingleNodeServerOptions.CommitLogOptions(
        closeDelayMs = Int.MaxValue))

  private val clientBuilder = new ClientBuilder()
  private val (zkServer, zkClient) = startZkServerAndGetIt

  override def afterAll(): Unit = {
    zkClient.close()
    zkServer.close()
  }


  "Server" should "not delete stream that doesn't exists" in {
    val bundle = startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder)

    bundle.operate { _ =>
      val client = bundle.client

      Await.result(client.delStream("test_stream"), responseTimeout) shouldBe false
    }
  }

  it should "put stream, then delete it properly" in {
    val bundle = startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder)

    bundle.operate { _ =>
      val client = bundle.client

      val stream = getRandomStream

      val streamID = Await.result(client.putStream(stream), responseTimeout)
      streamID shouldBe 0
      Await.result(client.checkStreamExists(stream.name), responseTimeout) shouldBe true
      Await.result(client.delStream(stream.name), responseTimeout) shouldBe true
      Await.result(client.checkStreamExists(stream.name), responseTimeout) shouldBe false
    }
  }

  it should "not delete stream if it is already deleted" in {
    val bundle = startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder)

    bundle.operate { _ =>
      val client = bundle.client
      val stream = getRandomStream

      val streamID = Await.result(client.putStream(stream), responseTimeout)
      streamID shouldBe 0
      Await.result(client.delStream(stream.name), responseTimeout) shouldBe true
      Await.result(client.checkStreamExists(stream.name), responseTimeout) shouldBe false
      Await.result(client.delStream(stream.name), responseTimeout) shouldBe false
    }
  }

  it should "put stream, then delete this stream, and server should save producer and consumer transactions on putting them by client" in {
    val bundle = startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder)

    bundle.operate { transactionServer =>
      val client = bundle.client

      val stream = getRandomStream
      val streamID = Await.result(client.putStream(stream), responseTimeout)
      val producerTransactions = Array.fill(100)(getRandomProducerTransaction(streamID, stream)).filter(_.state == TransactionStates.Opened)
      val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(streamID, stream))

      Await.result(client.putTransactions(producerTransactions, consumerTransactions), responseTimeout)

      //it's required to a CommitLogToRocksWriter writes the producer transactions to db
      transactionServer.scheduledCommitLog.run()
      transactionServer.commitLogToRocksWriter.run()

      val fromID = producerTransactions.minBy(_.transactionID).transactionID
      val toID = producerTransactions.maxBy(_.transactionID).transactionID

      val resultBeforeDeleting =
        Await.result(
          client.scanTransactions(streamID, stream.partitions, fromID, toID, Int.MaxValue, Set()), responseTimeout)
          .producerTransactions
      resultBeforeDeleting should not be empty

      Await.result(client.delStream(stream.name), responseTimeout)
      Await.result(client.scanTransactions(streamID, stream.partitions, fromID, toID, Int.MaxValue, Set()), responseTimeout)
        .producerTransactions should contain theSameElementsInOrderAs resultBeforeDeleting
    }
  }
}
