package com.bwsw.tstreams.coordination.pubsub.publisher

import java.net.InetSocketAddress
import com.bwsw.tstreams.coordination.pubsub.messages.ProducerTopicMessage
import io.netty.bootstrap.Bootstrap
import io.netty.channel.ChannelInitializer
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.string.{StringDecoder, StringEncoder}

/**
 * Broadcaster for [[com.bwsw.tstreams.agents.producer.BasicProducer]]]
 * to broadcast messages for all [[com.bwsw.tstreams.agents.consumer.subscriber.BasicSubscribingConsumer]]]
 */
class Broadcaster {
  private val group = new NioEventLoopGroup()
  private val channelHandler = new BroadcasterChannelHandler(this)
  private val bootstrap = new Bootstrap()

  bootstrap
    .group(group)
    .channel(classOf[NioSocketChannel])
    .handler(new ChannelInitializer[SocketChannel]() {
      override def initChannel(ch: SocketChannel) {
        val p = ch.pipeline()
        p.addLast("decoder", new StringDecoder())
        p.addLast("encoder", new StringEncoder())
        p.addLast("serializer", new ProducerTopicMessageEncoder())
        p.addLast("handler", channelHandler)
      }
    })

  /**
   * Connect to some subscriber
   * @param address Subscriber address in network
   */
  def connect(address : String) = {
    val splits = address.split(":")
    assert(splits.size == 2)
    val host = splits(0)
    val port = splits(1).toInt
    //TODO mb wrong according to https://habrahabr.ru/post/136456/
    //mb need to replace with approach below:
    //https://github.com/netty/netty/blob/b03b11a24860a1d636744c989dad50d223ffc6bc/src/main/java/org/jboss/netty/example/proxy/HexDumpProxyInboundHandler.java
    val channelFuture = bootstrap.connect(new InetSocketAddress(host,port)).sync()
    if (channelFuture.isSuccess)
      channelHandler.updateMap(channelFuture.channel().id(), address)
  }

  /**
   * Send msg to all connected subscribers
   * @param msg Msg to send
   */
  def broadcast(msg : ProducerTopicMessage) : Unit = {
    channelHandler.broadcast(msg)
  }

  /**
   * Close broadcaster
   */
  def close() : Unit = {
    group.shutdownGracefully().await()
  }

  /**
   * Update subscribers with new addresses
   * @param newSubscribers New subscribers list of addresses
   */
  def updateSubscribers(newSubscribers : List[String]) : Unit = {
    channelHandler.updateSubscribers(newSubscribers)
  }
}