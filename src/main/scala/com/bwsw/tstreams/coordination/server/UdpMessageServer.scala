package com.bwsw.tstreams.coordination.server

import java.util.concurrent.{CountDownLatch, TimeUnit}

import io.netty.bootstrap.Bootstrap
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.DatagramPacket
import io.netty.channel.socket.nio.NioDatagramChannel

/**
  * Created by ivan on 11.04.17.
  */
class UdpMessageServer(host: String, port: Int, handler: SimpleChannelInboundHandler[DatagramPacket]) {
  //channel workers
  private val workerGroup = new NioEventLoopGroup()
  private var listenerThread: Thread = null

  /**
    * Stop this listener
    */
  def stop(): Unit = {
    workerGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS).await()
  }


  /**
    * Start this listener
    */
  def start() = {
    assert(listenerThread == null || !listenerThread.isAlive)
    val syncPoint = new CountDownLatch(1)
    listenerThread = new Thread(() => {
      try {
        val b = new Bootstrap()
        b.group(workerGroup)
          .channel(classOf[NioDatagramChannel])
          .handler(handler)
        val f = b.bind(host, port).sync()
        syncPoint.countDown()
        f.channel().closeFuture().sync()
      } finally {
        workerGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS)
      }
    })
    listenerThread.start()
    syncPoint.await()
  }
}