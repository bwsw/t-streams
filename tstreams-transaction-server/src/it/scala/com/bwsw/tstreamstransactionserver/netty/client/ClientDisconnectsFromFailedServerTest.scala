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

package com.bwsw.tstreamstransactionserver.netty.client

import com.bwsw.tstreamstransactionserver.exception.Throwable.ServerConnectionException
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.SingleNodeServerBuilder
import com.bwsw.tstreamstransactionserver.options._
import com.bwsw.tstreamstransactionserver.util.Utils._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class ClientDisconnectsFromFailedServerTest
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


  "Client" should "throw an exception when the server isn't available" in {
    val transactionsCount = 10000
    val bundle = startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { transactionServer =>
      val client = bundle.client

      val stream = getRandomStream
      val streamID = Await.result(client.putStream(stream), responseTimeout)

      val producerTransactions = Array.fill(transactionsCount)(getRandomProducerTransaction(streamID, stream))
      val consumerTransactions = Array.fill(transactionsCount)(getRandomConsumerTransaction(streamID, stream))

      val resultInFuture = client.putTransactions(producerTransactions, consumerTransactions)

      transactionServer.shutdown()

      a[ServerConnectionException] shouldBe thrownBy {
        Await.result(resultInFuture, responseTimeout)
      }
    }
  }

  it should "throw an exception when the server restarted" in {
    val transactionsCount = 10000
    val connectionOptions =
      clientBuilder.getConnectionOptions.copy(requestTimeoutMs = responseTimeout.toMillis.toInt)
    val bundle = startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder.withConnectionOptions(connectionOptions)
    )

    val client = bundle.client
    val transactionServer = bundle.transactionServer

    val stream = getRandomStream
    val producerTransactions = Array.fill(transactionsCount)(getRandomProducerTransaction(1, stream))
    val consumerTransactions = Array.fill(transactionsCount)(getRandomConsumerTransaction(1, stream))


    val resultInFuture = client.putTransactions(producerTransactions, consumerTransactions)


    transactionServer.shutdown()
    val secondServer = bundle.serverBuilder
      .withBootstrapOptions(SingleNodeServerOptions.BootstrapOptions(bindPort = getRandomPort))
      .build()

    val task = new Thread(
      () => secondServer.start()
    )

    task.start()

    /*
     * The type of this exception can be either MasterLostException or ServerUnreachableException,
     * depends what happens earlier: updating of master node in ZooKeeper or
     * invocation InetClient.onServerConnectionLostDefaultBehaviour() from NettyConnectionHandler
     */
    a[ServerConnectionException] shouldBe thrownBy {
      Await.result(resultInFuture, responseTimeout)
    }

    secondServer.shutdown()
    task.interrupt()
    bundle.closeDbsAndDeleteDirectories()
  }

  it should "disconnect from the server when it is off" in {
    val waitingTimeout = 1000
    val bundle = startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { server =>
      val client = bundle.client

      Thread.sleep(waitingTimeout) // wait until client connected to server
      client.isConnected shouldBe true

      server.shutdown()
      Thread.sleep(waitingTimeout) // wait until client disconnected from server

      client.isConnected shouldBe false
    }
  }
}
