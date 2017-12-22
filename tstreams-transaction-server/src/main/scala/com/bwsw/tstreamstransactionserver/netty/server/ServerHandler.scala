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
package com.bwsw.tstreamstransactionserver.netty.server


import com.bwsw.tstreamstransactionserver.netty.RequestMessage
import com.bwsw.tstreamstransactionserver.netty.server.handler.RequestRouter
import com.bwsw.tstreamstransactionserver.tracing.ServerTracer.tracer
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import org.slf4j.LoggerFactory

@Sharable
class ServerHandler(requestRouter: RequestRouter)
  extends SimpleChannelInboundHandler[ByteBuf] {
  private val logger =
    LoggerFactory.getLogger(this.getClass)

  override def channelRead0(ctx: ChannelHandlerContext, buf: ByteBuf): Unit = {
    val message = RequestMessage.fromByteBuf(buf)
    tracer.serverReceive(message)
    invokeMethod(message, ctx)
  }

  protected def invokeMethod(message: RequestMessage, ctx: ChannelHandlerContext): Unit = {

    if (logger.isDebugEnabled)
      logger.debug(s"${ctx.channel().remoteAddress().toString} request id ${message.id} method is invoked.")

    requestRouter.route(message, ctx)

  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.channel().close()
  }
}