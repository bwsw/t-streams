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

package com.bwsw.tstreamstransactionserver.netty.server.handler.auth

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreamstransactionserver.netty.server.authService.AuthService
import com.bwsw.tstreamstransactionserver.netty.{Protocol, ResponseMessage}
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.AuthenticationOptions
import com.bwsw.tstreamstransactionserver.rpc.TransactionService
import io.netty.buffer.ByteBuf
import io.netty.channel._
import io.netty.channel.embedded.EmbeddedChannel
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random

/** Tests for [[KeepAliveHandler]]
  *
  * @author Pavel Tomskikh
  */
class KeepAliveHandlerTest extends FlatSpec with Matchers with MockitoSugar {

  "KeepAliveHandler" should "handle requests properly" in {
    val key = "key"
    val authService = new AuthService(AuthenticationOptions(key, 100, 10))
    val updatingResults = Map(
      authService.authenticate(key).get -> true,
      Random.nextInt() -> false)

    val keepAliveHandler = new KeepAliveHandler(authService)

    updatingResults.foreach {
      case (token, updatingResult) =>
        val expected = Protocol.KeepAlive.encodeResponse(
          TransactionService.KeepAlive.Result(Some(updatingResult)))
        val request = Protocol.KeepAlive.encodeRequestToMessage(
          TransactionService.KeepAlive.Args())(
          messageId = Random.nextLong(),
          token = token,
          isFireAndForgetMethod = false)


        val channel = new EmbeddedChannel()

        val latch = new CountDownLatch(1)
        val context = mock[ChannelHandlerContext]
        when(context.channel()).thenReturn(channel)
        when(context.alloc()).thenReturn(channel.alloc())
        when(context.writeAndFlush(any(), any())).thenAnswer(invocation => {
          val buffer = invocation.getArgument[ByteBuf](0)
          val response = ResponseMessage.fromByteBuf(buffer)
          response.body shouldEqual expected
          latch.countDown()

          mock[ChannelFuture]
        })

        keepAliveHandler.handle(request, context, None)
        channel.runPendingTasks()

        latch.await(1, TimeUnit.SECONDS) shouldBe true
    }
  }
}
