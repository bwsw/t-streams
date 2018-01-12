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

import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreamstransactionserver.netty.SocketHostPortPair
import com.bwsw.tstreamstransactionserver.options.ClientOptions.ConnectionOptions
import io.netty.bootstrap.Bootstrap
import io.netty.channel._
import io.netty.channel.epoll.{EpollEventLoopGroup, EpollSocketChannel}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.logging.LoggingHandler
import org.slf4j.LoggerFactory


class NettyConnection(workerGroup: EventLoopGroup,
                      master: SocketHostPortPair,
                      connectionOptions: ConnectionOptions,
                      handlers: => Seq[ChannelHandler],
                      onConnectionLostDo: => Unit) {

  private val logger =
    LoggerFactory.getLogger(this.getClass)

  private val isStopped =
    new AtomicBoolean(false)

  private val channelType = determineChannelType()

  private val bootstrap: Bootstrap = {
    new Bootstrap()
      .group(workerGroup)
      .channel(channelType)
      .option(
        ChannelOption.SO_KEEPALIVE,
        java.lang.Boolean.FALSE
      )
      .option(
        ChannelOption.CONNECT_TIMEOUT_MILLIS,
        int2Integer(connectionOptions.connectionTimeoutMs)
      )
      .handler(new ChannelInitializer[SocketChannel] {
        override def initChannel(ch: SocketChannel): Unit = {
          val pipeline = ch.pipeline()

          handlers.foreach(handler =>
            pipeline.addLast(handler)
          )

          pipeline.addFirst(new LoggingHandler())

          pipeline.addLast(
            new NettyConnectionHandler(onConnectionLostDo))
        }
      })
  }


  private val channel: ChannelFuture = {
    logger.info(s"Start a connection to $master")

    bootstrap
      .connect(master.address, master.port)
      .awaitUninterruptibly()
  }

  private def determineChannelType(): Class[_ <: SocketChannel] =
    workerGroup match {
      case _: EpollEventLoopGroup => classOf[EpollSocketChannel]
      case _: NioEventLoopGroup => classOf[NioSocketChannel]
      case group => throw new IllegalArgumentException(
        s"Can't determine channel type for group '$group'."
      )
    }

  final def getChannel(): Channel = {
    channel.channel()
  }

  def stop(): Unit = {
    logger.info(s"Close a connection to $master")

    val isNotStopped =
      isStopped.compareAndSet(false, true)
    if (isNotStopped) {
      scala.util.Try(
        channel.channel()
          .close()
          .cancel(true)
      )
    }
  }
}
