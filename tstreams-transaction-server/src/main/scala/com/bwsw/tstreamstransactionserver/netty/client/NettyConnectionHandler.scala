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

import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import org.slf4j.LoggerFactory

@Sharable
class NettyConnectionHandler(onServerConnectionLostDo: => Unit)
  extends ChannelInboundHandlerAdapter {

  private val logger =
    LoggerFactory.getLogger(this.getClass)


  override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = {
    ctx.fireChannelRead(msg)
  }

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    if (logger.isInfoEnabled)
      logger.info(s"Connected to: ${ctx.channel().remoteAddress()}")
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    if (logger.isInfoEnabled)
      logger.info(s"Disconnected from: ${ctx.channel().remoteAddress()}")
  }

  @throws[Exception]
  override def channelUnregistered(ctx: ChannelHandlerContext): Unit = {
    scala.util.Try(onServerConnectionLostDo) match {
      case scala.util.Failure(throwable) =>
        ctx.close()
        throw throwable
      case _ =>
        ctx.close()
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext,
                               cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }
}
