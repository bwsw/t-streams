package com.bwsw.tstreams.coordination.pubsub.listener

import java.util

import com.bwsw.tstreams.common.ProtocolMessageSerializer
import com.bwsw.tstreams.common.ProtocolMessageSerializer.ProtocolMessageSerializerException
import com.bwsw.tstreams.coordination.pubsub.messages.ProducerTopicMessage
import com.bwsw.tstreams.coordination.pubsub.subscriber.{ProducerEventReceiverTcpServer, CallbackManager}
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import io.netty.handler.codec.MessageToMessageDecoder
import io.netty.util.ReferenceCountUtil
import org.slf4j.LoggerFactory


/**
  * Incoming connections manager for [[ProducerEventReceiverTcpServer]]]
  */
@Sharable
class SubscriberChannelHandler(subscriberManager: CallbackManager) extends SimpleChannelInboundHandler[ProducerTopicMessage] {
  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Triggered when new connection accept
    *
    * @param ctx Netty ctx
    */
  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    subscriberManager.incrementCount()
  }

  /**
    * Triggered when new message [[ProducerTopicMessage]]] received
    *
    * @param ctx Netty ctx
    * @param msg [[ProducerTopicMessage]]]
    */
  override def channelRead0(ctx: ChannelHandlerContext, msg: ProducerTopicMessage): Unit = {
    logger.debug(s"[READ PARTITION_${msg.partition}] ts=${msg.txnUuid.timestamp()} ttl=${msg.ttl} status=${msg.status}")
    subscriberManager.invokeCallbacks(msg)
    ReferenceCountUtil.release(msg)
  }

  /**
    * Triggered on exceptions
    *
    * @param ctx   Netty ctx
    * @param cause Exception cause
    */
  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) = {
    logger.error(s"SubscriberListener exception : ${cause.getMessage}")
  }
}

/**
  * Decoder to convert [[java.lang.String]] to [[ProducerTopicMessage]]]
  */
class ProducerTopicMessageDecoder extends MessageToMessageDecoder[String] {
  val logger = LoggerFactory.getLogger(this.getClass)
  val serializer = new ProtocolMessageSerializer

  override def decode(ctx: ChannelHandlerContext, msg: String, out: util.List[AnyRef]): Unit = {
    try {
      if (msg != null)
        out.add(serializer.deserialize[ProducerTopicMessage](msg))
    }
    catch {
      case e: ProtocolMessageSerializerException =>
        logger.warn(s"TStreams Serializer Exception: ${e.getMessage}")
    }
  }
}
