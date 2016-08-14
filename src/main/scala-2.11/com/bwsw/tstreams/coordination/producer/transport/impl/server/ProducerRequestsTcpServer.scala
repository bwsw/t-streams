package com.bwsw.tstreams.coordination.producer.transport.impl.server

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.common.ProtocolMessageSerializer
import com.bwsw.tstreams.coordination.messages.master.IMessage
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.{ChannelInitializer, SimpleChannelInboundHandler}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.string.{StringDecoder, StringEncoder}
import io.netty.handler.codec.{DelimiterBasedFrameDecoder, Delimiters}
import io.netty.handler.logging.{LogLevel, LoggingHandler}

/**
  * @param port Listener port
  */
class ProducerRequestsTcpServer(host: String, port: Int, handler: SimpleChannelInboundHandler[String]) {
  //socket accept worker
  private val bossGroup = new NioEventLoopGroup(1)
  //channel workers
  private val workerGroup = new NioEventLoopGroup()
  private val MAX_FRAME_LENGTH = 8192
  private var listenerThread: Thread = null

  /**
    * Stop this listener
    */
  def stop(): Unit = {
    workerGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS).await()
    bossGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS).await()
  }


  /**
    * Response with [[IMessage]]]
    */
  def respond(msg: IMessage): Unit = {
    msg.channel.writeAndFlush(
      ProtocolMessageSerializer.wrapMsg(
        ProtocolMessageSerializer.serialize(msg)))
  }

  /**
    * Start this listener
    */
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
                ch.config().setTcpNoDelay(true)
                ch.config().setKeepAlive(true)
                ch.config().setTrafficClass(0x10)
                ch.config().setPerformancePreferences(0,1,0)
                val p = ch.pipeline()
                p.addLast("framer", new DelimiterBasedFrameDecoder(MAX_FRAME_LENGTH, Delimiters.lineDelimiter(): _*))
                p.addLast("decoder", new StringDecoder())
                p.addLast("encoder", new StringEncoder())
                p.addLast("handler", handler)
              }
            })
          val f = b.bind(host, port).sync()
          syncPoint.countDown()
          f.channel().closeFuture().sync()
        } finally {
          workerGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS)
          bossGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS)
        }
      }
    })
    listenerThread.start()
    syncPoint.await()
  }
}