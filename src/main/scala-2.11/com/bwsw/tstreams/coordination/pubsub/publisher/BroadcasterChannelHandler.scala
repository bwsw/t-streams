package com.bwsw.tstreams.coordination.pubsub.publisher

import java.util

import com.bwsw.tstreams.common.serializer.TStreamsSerializer
import com.bwsw.tstreams.coordination.pubsub.messages.ProducerTopicMessage
import io.netty.channel._
import io.netty.channel.group.{ChannelGroupFuture, ChannelGroupFutureListener, DefaultChannelGroup}
import io.netty.handler.codec.MessageToMessageEncoder
import io.netty.util.concurrent.GlobalEventExecutor
import org.slf4j.LoggerFactory


@ChannelHandler.Sharable
class BroadcasterChannelHandler(connectionManager: BroadcasterConnectionManager)
  extends SimpleChannelInboundHandler[ProducerTopicMessage] {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val group = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE)

  /**
    * Triggered on read from channel (incorrect state because broadcaster must only broadcast)
    */
  override def channelRead0(ctx: ChannelHandlerContext, msg: ProducerTopicMessage): Unit = {
    logger.warn("[BroadcasterChannelHandler] Broadcaster must only broadcast messages")
  }

  /**
    * Triggered on connect to new subscriber
    *
    * @param ctx Netty ctx
    */
  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    group.add(ctx.channel())
  }

  /**
    * Triggered on disconnect of subscriber
    *
    * @param ctx Netty ctx
    */
  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    val id = ctx.channel().id()
    connectionManager.channelInactive(id)
  }

  /**
    * Triggered on exception
    *
    * @param ctx   Netty ctx
    * @param cause Cause of exception
    */
  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) = {
    println(s"Broadcaster exception : ${cause.getMessage}")
    //    ctx.close()
  }

  /**
    * Broadcast msg to all subscribers
    *
    * @param msg Msg to broadcast
    */
  def broadcast(msg: ProducerTopicMessage, onComplete: () => Unit): Unit = {
    logger.debug(s"[BROADCASTER PUBLISH] partition=${msg.partition} status=${msg.status} uuid=${msg.txnUuid.timestamp()}")
    val cf = group.writeAndFlush(msg)
    cf.addListener(new ChannelGroupFutureListener {
      override def operationComplete(future: ChannelGroupFuture): Unit = {
        onComplete()
      }
    })
  }
}

/**
  * Encoder [[ProducerTopicMessage]]] to [[java.lang.String]]]
  */
class ProducerTopicMessageEncoder extends MessageToMessageEncoder[ProducerTopicMessage] {
  val serializer = new TStreamsSerializer

  override def encode(ctx: ChannelHandlerContext, msg: ProducerTopicMessage, out: util.List[AnyRef]): Unit = {
    out.add(serializer.serialize(msg) + "\n")
  }
}