package com.bwsw.tstreams.coordination.pubsub.publisher

import java.util.concurrent.TimeUnit

import com.bwsw.tstreams.coordination.pubsub.messages.ProducerTopicMessage
import io.netty.bootstrap.Bootstrap
import io.netty.channel.ChannelInitializer
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.string.{StringDecoder, StringEncoder}

/**
  * Broadcaster for [[com.bwsw.tstreams.agents.producer.Producer]]]
  * to broadcast messages for all [[com.bwsw.tstreams.agents.consumer.subscriber.SubscribingConsumer]]]
  */
class Broadcaster {
  private val group = new NioEventLoopGroup()
  private val bootstrap = new Bootstrap()
  private val connectionManager = new BroadcasterConnectionManager(bootstrap)
  private val channelHandler = new BroadcasterChannelHandler(connectionManager)

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
    * Send msg to all connected subscribers
    *
    * @param msg Msg to send
    */
  def broadcast(msg: ProducerTopicMessage, onComplete: () => Unit): Unit = {
    channelHandler.broadcast(msg, onComplete)
  }

  /**
    * Update subscribers with new set of subscribers
    *
    * @param subscribers New subscribers
    */
  def updateSubscribers(subscribers: List[String]) = {
    connectionManager.updateSubscribers(subscribers)
  }

  /**
    * Close broadcaster
    */
  def close(): Unit = {
    group.shutdownGracefully(0, 0, TimeUnit.SECONDS).await()
  }
}