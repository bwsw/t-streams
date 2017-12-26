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

import com.bwsw.tstreamstransactionserver.exception.Throwable.InvalidSocketAddress
import com.bwsw.tstreamstransactionserver.netty.SocketHostPortPair
import io.netty.channel.EventLoopGroup
import io.netty.channel.epoll.{Epoll, EpollEventLoopGroup, EpollServerSocketChannel}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.ServerSocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel

object Util {

  @throws[InvalidSocketAddress]
  final def createTransactionServerExternalSocket(host: String,
                                                  port: Int): SocketHostPortPair = {

    if (!SocketHostPortPair.isValid(host, port)) {
      throw new InvalidSocketAddress(
        s"Address $host:$port is not a correct socket address pair."
      )
    }

    val externalHost = System.getenv("HOST")
    val externalPort = System.getenv("PORT0")

    SocketHostPortPair
      .fromString(s"$externalHost:$externalPort")
      .orElse(
        SocketHostPortPair.validateAndCreate(
          host,
          port
        )
      )
      .getOrElse {
        if (externalHost == null || externalPort == null)
          throw new InvalidSocketAddress(
            s"Socket $host:$port is not valid for external access."
          )
        else
          throw new InvalidSocketAddress(
            s"Environment parameters 'HOST':'PORT0' " +
              s"$externalHost:$externalPort are not valid for a socket."
          )
      }
  }

  private def determineChannelType(workerGroup: EventLoopGroup): Class[_ <: ServerSocketChannel] = {
    workerGroup match {
      case _: EpollEventLoopGroup =>
        classOf[EpollServerSocketChannel]
      case _: NioEventLoopGroup =>
        classOf[NioServerSocketChannel]
      case group => throw new IllegalArgumentException(
        s"Can't determine channel type for group '$group'."
      )
    }
  }

  final def getBossGroupAndWorkerGroupAndChannel: (EventLoopGroup, EventLoopGroup, Class[_ <: ServerSocketChannel]) = {
    val (bossGroup: EventLoopGroup, workerGroup: EventLoopGroup) = {
      if (Epoll.isAvailable)
        (new EpollEventLoopGroup(1), new EpollEventLoopGroup())
      else
        (new NioEventLoopGroup(1), new NioEventLoopGroup())
    }

    val channelType =
      determineChannelType(workerGroup)


    (bossGroup, workerGroup, channelType)
  }
}
