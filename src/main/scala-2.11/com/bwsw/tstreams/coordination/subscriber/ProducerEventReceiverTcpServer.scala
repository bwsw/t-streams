package com.bwsw.tstreams.coordination.subscriber

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.coordination.messages.state.Message
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.ChannelInitializer
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.string.StringDecoder
import io.netty.handler.codec.{DelimiterBasedFrameDecoder, Delimiters}
import io.netty.handler.logging.{LogLevel, LoggingHandler}

/**
  * Listener of [[Message]]
  *
  * @param port Listener port
  */
class ProducerEventReceiverTcpServer(host: String, port: Int) {
  private val bossGroup = new NioEventLoopGroup(1)
  private val workerGroup = new NioEventLoopGroup()
  private val MAX_FRAME_LENGTH = 8192
  private val callbackManager = new CallbackManager()
  private val channelHandler: ChannelHandler = new ChannelHandler(callbackManager)
  private var listenerThread: Thread = null
  private val isStopped = new AtomicBoolean(false)

  /**
    * Stop to listen [[Message]]]
    */
  def stop(): Unit = {
    if(isStopped.getAndSet(true))
      throw new IllegalStateException("ProducerEventReceiver is stopped already. Second try to stop.")

    workerGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS).await()
    bossGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS).await()
  }

  /**
    * Add new event to [[channelHandler]]]
    *
    * @param callback Event callback
    */
  def addCallbackToChannelHandler(callback: (Message) => Unit): Unit = {
    if(isStopped.get)
      throw new IllegalStateException("ProducerEventReceiver is stopped already.")
    callbackManager.addCallback(callback)
  }

  /**
    * Retrieve count of accepted
    * connection managed by [[channelHandler]]]
    */
  def getConnectionsAmount(): Int = {
    if(isStopped.get)
      throw new IllegalStateException("ProducerEventReceiver is stopped already.")
    callbackManager.getCount()
  }

  def resetConnectionsAmount(): Unit = {
    if(isStopped.get)
      throw new IllegalStateException("ProducerEventReceiver is stopped already.")
    callbackManager.resetCount()
  }

  /**
    * Start this listener
    */
  def start(): Unit = {
    if(isStopped.get)
      throw new IllegalStateException("ProducerEventReceiver is stopped already.")

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
                p.addLast("framer", new DelimiterBasedFrameDecoder(MAX_FRAME_LENGTH, Delimiters.lineDelimiter(): _*))
                p.addLast("decoder", new StringDecoder())
                p.addLast("deserializer", new ProducerTopicMessageDecoder())
                p.addLast("handler", channelHandler)
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
