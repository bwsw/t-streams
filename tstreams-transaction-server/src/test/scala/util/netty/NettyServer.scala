package util.netty

import java.util.concurrent.{CountDownLatch, TimeUnit}

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.epoll.{Epoll, EpollEventLoopGroup, EpollServerSocketChannel}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.{ChannelInitializer, ChannelOption, EventLoopGroup}
import util.netty.NettyServer._

private object NettyServer {
  def createEventLoopGroup(): EventLoopGroup = {
    if (Epoll.isAvailable) {
      new EpollEventLoopGroup()
    }
    else {
      new NioEventLoopGroup()
    }
  }

  def createEventLoopGroup(threadNumber: Int): EventLoopGroup = {
    if (Epoll.isAvailable) {
      new EpollEventLoopGroup(threadNumber)
    }
    else {
      new NioEventLoopGroup(threadNumber)
    }
  }

}

final class NettyServer(host: String,
                        port: Int,
                        channelInitializer: ChannelInitializer[SocketChannel] = new NettyServerInitializer()) {

  private lazy val bossGroup: EventLoopGroup =
    createEventLoopGroup(1)

  private lazy val workerGroup: EventLoopGroup =
    createEventLoopGroup()

  def start(): Unit = {
    val latch = new CountDownLatch(1)
    new Thread(() => {
      val bootstrap = new ServerBootstrap()
      bootstrap.group(bossGroup, workerGroup)
        .channel(classOf[EpollServerSocketChannel])
        .childHandler(channelInitializer)
        .childOption[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, false)

      val binding = bootstrap
        .bind(host, port)
        .sync()

      val channel = binding
        .channel()
        .closeFuture()

      latch.countDown()

      channel.sync()

    }).start()

    val isBinded =
      latch.await(3000, TimeUnit.MILLISECONDS)

    if (!isBinded)
      throw new IllegalStateException(s"Netty Test Server isn't binded to $host:$port")
  }

  def shutdown(): Unit = {
    scala.util.Try {
      bossGroup.shutdownGracefully(
        0L,
        0L,
        TimeUnit.NANOSECONDS
      ).awaitUninterruptibly(100L)
    }

    scala.util.Try {
      workerGroup.shutdownGracefully(
        0L,
        0L,
        TimeUnit.NANOSECONDS
      ).awaitUninterruptibly(100L)
    }
  }
}
