package com.bwsw.tstreams.coordination.pubsub.listener

import java.util

import com.bwsw.tstreams.common.serializer.TStreamsSerializer
import com.bwsw.tstreams.common.serializer.TStreamsSerializer.TStreamsSerializerException
import com.bwsw.tstreams.coordination.pubsub.messages.ProducerTopicMessage
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import io.netty.handler.codec.MessageToMessageDecoder
import io.netty.util.ReferenceCountUtil
import org.slf4j.LoggerFactory


/**
 * Incoming connections manager for [[SubscriberListener]]]
 */
@Sharable
class SubscriberChannelHandler(subscriberManager: SubscriberManager) extends SimpleChannelInboundHandler[ProducerTopicMessage] {
  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Triggered when new connection accept
   * @param ctx Netty ctx
   */
  override def channelActive(ctx: ChannelHandlerContext) : Unit = {
    subscriberManager.incrementCount()
  }

  /**
   * Triggered when new message [[ProducerTopicMessage]]] received
   * @param ctx Netty ctx
   * @param msg [[ProducerTopicMessage]]]
   */
  override def channelRead0(ctx: ChannelHandlerContext, msg: ProducerTopicMessage): Unit = {
    logger.debug(s"[READ PARTITION_${msg.partition}] ts=${msg.txnUuid.timestamp()} ttl=${msg.ttl} status=${msg.status}\n")
    subscriberManager.invokeCallbacks(msg)
    ReferenceCountUtil.release(msg)
  }

  /**
   * Triggered on exceptions
   * @param ctx Netty ctx
   * @param cause Exception cause
   */
  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) = {
    println(s"SubscriberListener exception : ${cause.getMessage}")
//    ctx.close()
  }
}

/**
 * Decoder to convert [[java.lang.String]] to [[ProducerTopicMessage]]]
 */
class ProducerTopicMessageDecoder extends MessageToMessageDecoder[String]{
  val logger = LoggerFactory.getLogger(this.getClass)
  val serializer = new TStreamsSerializer

  override def decode(ctx: ChannelHandlerContext, msg: String, out: util.List[AnyRef]): Unit = {
    try {
      if (msg != null)
        out.add(serializer.deserialize[ProducerTopicMessage](msg))
    }
    catch {
      case e : TStreamsSerializerException =>
        logger.warn(s"TStreams Serializer Exception: ${e.getMessage}\n")
    }
  }
}
