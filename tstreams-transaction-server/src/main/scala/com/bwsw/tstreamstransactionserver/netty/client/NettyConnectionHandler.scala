package com.bwsw.tstreamstransactionserver.netty.client

import java.util.concurrent.TimeUnit

import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import org.slf4j.LoggerFactory

@Sharable
class NettyConnectionHandler(reconnectDelayMs: Int,
                             onServerConnectionLostDo: => Unit,
                             connectMethod: => Unit)
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
    if (logger.isInfoEnabled)
      logger.info(s"Sleeping for: " + reconnectDelayMs + "ms")


    scala.util.Try(onServerConnectionLostDo) match {
      case scala.util.Failure(throwable) =>
        ctx.close()
        throw throwable
      case _ =>
        ctx.channel.eventLoop.schedule(new Runnable() {
          override def run(): Unit = {
            if (logger.isInfoEnabled)
              logger.info(s"Reconnecting to: ${ctx.channel.remoteAddress()}")
            connectMethod
          }
        }, reconnectDelayMs, TimeUnit.MILLISECONDS)
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext,
                               cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }
}
