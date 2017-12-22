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

import java.util.concurrent.ConcurrentMap

import com.bwsw.tstreamstransactionserver.netty.ResponseMessage
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}

import scala.concurrent.Promise


@Sharable
class ClientHandler(reqIdToRep: ConcurrentMap[Long, Promise[ByteBuf]])
  extends SimpleChannelInboundHandler[ByteBuf] {

  override def channelRead0(ctx: ChannelHandlerContext, buf: ByteBuf): Unit = {
    val id = ResponseMessage.getIdFromByteBuf(buf)
    val request = reqIdToRep.get(id)
    if (request != null) {
      val bufferToRead =
        buf.readRetainedSlice(
          buf.readableBytes()
        )
      request.trySuccess(bufferToRead)
    }
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    ctx.fireChannelInactive()
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }
}