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
import com.bwsw.tstreamstransactionserver.rpc.TransactionInfo
import com.bwsw.tstreamstransactionserver.util.Utils.{getRandomStream, startZookeeperServer, _}

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class SingleNodeServerGetTransactionTest
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll {
  private val secondsToWait = 10.seconds

  private lazy val serverBuilder =
    new SingleNodeServerBuilder()

  private lazy val clientBuilder =
    new ClientBuilder()


  private lazy val (zkServer, zkClient) =
    startZookeeperServer

  override def beforeAll(): Unit = {
    zkServer
    zkClient
  }

  override def afterAll(): Unit = {
    zkClient.close()
    zkServer.close()
  }

  "Client" should "get transaction ID that not less that current time" in {
    val bundle = startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { _ =>
      val client = bundle.client


      val currentTime = System.currentTimeMillis()
      val result = Await.result(client.getTransaction(), secondsToWait)


      result shouldBe >=(currentTime)
    }
  }

  it should "get transaction ID by timestamp" in {
    val bundle = startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { _ =>
      val client = bundle.client

      val currentTime = System.currentTimeMillis()
      val result = Await.result(client.getTransaction(currentTime), secondsToWait)

      result shouldBe (currentTime * 100000)
    }
  }

  it should "not get a transaction if it does not exists" in {
    val bundle = startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { _ =>
      val client = bundle.client

      val stream = getRandomStream
      val fakeTransactionID = System.nanoTime()

      val streamID = Await.result(client.putStream(stream), secondsToWait)
      val response = Await.result(client.getTransaction(streamID, stream.partitions, fakeTransactionID), secondsToWait)

      response shouldBe TransactionInfo(exists = false, None)
    }
  }
}
