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

import java.net.Socket

import com.bwsw.tstreamstransactionserver.netty.client.ClientBuilder
import com.bwsw.tstreamstransactionserver.netty.{Protocol, ResponseMessage}
import com.bwsw.tstreamstransactionserver.options._
import com.bwsw.tstreamstransactionserver.rpc._
import com.bwsw.tstreamstransactionserver.util.Utils
import com.bwsw.tstreamstransactionserver.util.Utils.startZookeeperServer
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.util.Random

class ServerUpdatesTokenTtlTest
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll {

  private val serverBuilder = new SingleNodeServerBuilder()
    .withCommitLogOptions(
      SingleNodeServerOptions.CommitLogOptions(
        closeDelayMs = Int.MaxValue))

  private val clientBuilder = new ClientBuilder()
  private val (zkServer, zkClient) = startZookeeperServer

  override def afterAll(): Unit = {
    zkClient.close()
    zkServer.close()
  }


  "Server" should "update client's token TTL until client is disconnect" in {
    val ttlSec = 2
    val ttlMs = ttlSec * 1000
    val keepAliveThreshold = 3
    val keepAliveIntervalMs = ttlMs / keepAliveThreshold
    val authenticationOptions = serverBuilder.getAuthenticationOptions.copy(
      tokenTtlSec = ttlSec)
    val connectionOptions = clientBuilder.getConnectionOptions.copy(
      keepAliveIntervalMs = keepAliveIntervalMs,
      keepAliveThreshold = keepAliveThreshold)

    // TODO: find other way to retrieve client token
    val seed = 0
    Random.setSeed(seed)
    Random.nextInt()
    val token = Random.nextInt()
    Random.setSeed(seed)

    val bundle = Utils.startTransactionServerAndClient(
      zkClient,
      serverBuilder.withAuthenticationOptions(authenticationOptions),
      clientBuilder.withConnectionOptions(connectionOptions))

    bundle.operate { _ =>
      val bootstrapOptions = bundle.serverBuilder.getBootstrapOptions
      tokenIsValid(token, bootstrapOptions.bindHost, bootstrapOptions.bindPort) shouldBe true
      Thread.sleep(ttlMs)
      tokenIsValid(token, bootstrapOptions.bindHost, bootstrapOptions.bindPort) shouldBe true
      bundle.client.shutdown()
      tokenIsValid(token, bootstrapOptions.bindHost, bootstrapOptions.bindPort) shouldBe true
      Thread.sleep(ttlMs)
      tokenIsValid(token, bootstrapOptions.bindHost, bootstrapOptions.bindPort) shouldBe false
    }
  }


  private def tokenIsValid(token: Int, host: String, port: Int): Boolean = {
    val request = Protocol.IsValid.encodeRequestToMessage(
      TransactionService.IsValid.Args(token))(
      1L,
      token,
      isFireAndForgetMethod = false)

    val bytes = request.toByteArray
    val socket = new Socket(host, port)
    val inputStream = socket.getInputStream
    val outputStream = socket.getOutputStream
    outputStream.write(bytes)
    outputStream.flush()

    val waitResponseTimeout = 100

    def loop(lost: Int): Unit = {
      if (lost > 0 && inputStream.available() == 0) {
        Thread.sleep(waitResponseTimeout)
        loop(lost - 1)
      }
    }

    loop(10)

    val responseBytes = new Array[Byte](inputStream.available())
    inputStream.read(responseBytes)
    socket.close()

    val response = ResponseMessage.fromByteArray(responseBytes)
    val result = Protocol.IsValid.decodeResponse(response)

    result.success shouldBe defined

    result.success.get
  }
}
