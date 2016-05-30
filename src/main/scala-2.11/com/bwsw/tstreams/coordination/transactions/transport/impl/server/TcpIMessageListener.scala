package com.bwsw.tstreams.coordination.transactions.transport.impl.server

import java.util.concurrent.CountDownLatch
import com.bwsw.tstreams.coordination.transactions.messages.IMessage
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.ChannelInitializer
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.string.{StringDecoder, StringEncoder}
import io.netty.handler.codec.{DelimiterBasedFrameDecoder, Delimiters}
import io.netty.handler.logging.{LogLevel, LoggingHandler}

/**
 * @param port Listener port
 */
class TcpIMessageListener(port : Int){
  /**
   * Socket accept worker
   */
  private val bossGroup = new NioEventLoopGroup(1)
  /**
   * Channel workers
   */
  private val workerGroup = new NioEventLoopGroup()
  private val MAX_FRAME_LENGTH = 8192
  private val channelHandler : IMessageServerChannelHandler = new IMessageServerChannelHandler
  private var listenerThread : Thread = null

  def stop() = {
    workerGroup.shutdownGracefully().await()
    bossGroup.shutdownGracefully().await()
  }

  def addCallback(callback : (IMessage) => Unit) = {
    channelHandler.addCallback(callback)
  }

  def response(msg : IMessage) = {
    channelHandler.response(msg)
  }

  def start() = {
    assert(listenerThread == null || !listenerThread.isAlive)
    val syncPoint = new CountDownLatch(1)
    listenerThread = new Thread(new Runnable {
      override def run(): Unit = {
        try {
          val b = new ServerBootstrap()
          b.group(bossGroup, workerGroup).channel(classOf[NioServerSocketChannel])
            .handler(new LoggingHandler(LogLevel.DEBUG))
            .childHandler(new ChannelInitializer[SocketChannel]() {
              override def initChannel(ch: SocketChannel) {
                val p = ch.pipeline()
                p.addLast("framer", new DelimiterBasedFrameDecoder(MAX_FRAME_LENGTH, Delimiters.lineDelimiter():_*))
                p.addLast("decoder", new StringDecoder())
                p.addLast("deserializer", new IMessageDecoder())
                p.addLast("encoder", new StringEncoder())
                p.addLast("serializer", new IMessageEncoder())
                p.addLast("handler", channelHandler)
              }
            })
          val f = b.bind(port).sync()
          syncPoint.countDown()
          f.channel().closeFuture().sync()
        } finally {
          workerGroup.shutdownGracefully()
          bossGroup.shutdownGracefully()
        }
      }
    })
    listenerThread.start()
    syncPoint.await()
  }
}