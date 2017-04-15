package com.bwsw.tstreams.coordination.client

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreams.agents.producer.Producer
import com.bwsw.tstreams.common.FirstFailLockableTaskExecutor
import com.bwsw.tstreams.coordination.messages.master._
import com.bwsw.tstreams.coordination.server.RequestsTcpServer
import io.netty.channel.{Channel, ChannelHandler, ChannelHandlerContext, SimpleChannelInboundHandler}
import io.netty.util.ReferenceCountUtil

/**
  * Transport implementation
  */
class TcpNewTransactionServer(address: String) {
  val isIgnore = new AtomicBoolean(false)
  var callback: (Channel, String) => Unit = null

  def getInetAddress() = address

  @ChannelHandler.Sharable
  class ChannelHandler extends SimpleChannelInboundHandler[String] {
    override def channelRead0(ctx: ChannelHandlerContext, msg: String): Unit = {
      if (!isIgnore.get)
        executor.submit("<NettyNewMessageTask>", () => {
          callback(ctx.channel, msg)
          ReferenceCountUtil.release(msg)
        }, None)
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) = {
      IMessage.logger.error(cause.getMessage)
    }
  }

  private val executor = new FirstFailLockableTaskExecutor("PeerAgent-TcpTransportExecutor")
  private val splits = address.split(":")
  private val server: RequestsTcpServer = new RequestsTcpServer(splits(0), splits(1).toInt, new ChannelHandler())


  /**
    * Bind local agent address in transport
    */
  def start(callback: (Channel, String) => Unit): Unit = {
    this.callback = callback
    server.start()
  }

  /**
    * Stops server
    */
  def stopServer(): Unit = {
    IMessage.logger.info(s"Transport (for server) is shutting down.")
    server.stop()
    executor.shutdownOrDie(Producer.SHUTDOWN_WAIT_MAX_SECONDS, TimeUnit.SECONDS)
  }

}