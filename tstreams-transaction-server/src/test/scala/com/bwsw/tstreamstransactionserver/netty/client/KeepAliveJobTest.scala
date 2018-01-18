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

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreamstransactionserver.exception.Throwable.KeepAliveFailedException
import com.bwsw.tstreamstransactionserver.options.ClientOptions.ConnectionOptions
import com.bwsw.tstreamstransactionserver.rpc.TransactionService.KeepAlive
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/** Tests for [[KeepAliveJob]]
  *
  * @author Pavel Tomskikh
  */
class KeepAliveJobTest extends FlatSpec with Matchers with MockitoSugar {

  "KeepAliveJob" should "work properly" in {
    val client = mock[InetClient]
    val connectionOptions = ConnectionOptions(
      keepAliveIntervalMs = 100,
      keepAliveThreshold = 3)

    val results: Seq[Future[Option[Boolean]]] = Seq(
      true,
      true,
      false,
      true,
      false,
      false,
      false,
      true)
      .map(Some(_))
      .map(Future(_))

    when(client.isConnected).thenReturn(true)
    when(client.method[KeepAlive.Args, KeepAlive.Result, Option[Boolean]](
      any(), any(), any())(any()))
      .thenReturn(results.head, results.tail: _*)

    val latch = new CountDownLatch(1)
    val keepAliveJob = new KeepAliveJob(connectionOptions, client, exception => {
      latch.countDown()
      exception shouldBe a[KeepAliveFailedException]
    })

    val waitingInterval = (results.length - 1) * connectionOptions.keepAliveIntervalMs - 10
    keepAliveJob.start()
    Thread.sleep(waitingInterval) // keepAliveJob works
    latch.getCount shouldBe 1
    latch.await(connectionOptions.keepAliveIntervalMs * 2, TimeUnit.MILLISECONDS) shouldBe true
  }


  it should "interrupt properly" in {
    val client = mock[InetClient]
    val connectionOptions = ConnectionOptions(
      keepAliveIntervalMs = 50,
      keepAliveThreshold = 3)

    when(client.isConnected).thenReturn(true)
    when(
      client.method[KeepAlive.Args, KeepAlive.Result, Option[Boolean]](any(), any(), any())(any()))
      .thenReturn(Future(Some(true)))

    val latch = new CountDownLatch(1)
    val keepAliveJob = new KeepAliveJob(connectionOptions, client, exception => {
      latch.countDown()
      exception shouldBe a[KeepAliveFailedException]
    })

    val keepAliveRequests = 5
    val waitingInterval = connectionOptions.keepAliveIntervalMs * keepAliveRequests - 10
    keepAliveJob.start()
    Thread.sleep(waitingInterval) // keepAliveJob works
    keepAliveJob.stop()
    Thread.sleep(waitingInterval) // keepAliveJob stops

    verify(client, times(keepAliveRequests))
      .method[KeepAlive.Args, KeepAlive.Result, Option[Boolean]](any(), any(), any())(any())
    latch.getCount shouldBe 1

  }

}
