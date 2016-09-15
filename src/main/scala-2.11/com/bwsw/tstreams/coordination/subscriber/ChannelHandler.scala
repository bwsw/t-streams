package com.bwsw.tstreams.coordination.subscriber

import java.util

import com.bwsw.tstreams.common.ProtocolMessageSerializer
import com.bwsw.tstreams.common.ProtocolMessageSerializer.ProtocolMessageSerializerException
import com.bwsw.tstreams.coordination.messages.state.TransactionStateMessage
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import io.netty.handler.codec.MessageToMessageDecoder
import io.netty.util.ReferenceCountUtil
import org.slf4j.LoggerFactory


/**
  * Incoming connections manager
  */
@Sharable
class ChannelHandler(subscriberManager: CallbackManager) extends SimpleChannelInboundHandler[TransactionStateMessage] {
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
    * Triggered when new message received
    *
    * @param ctx Netty ctx
    * @param msg Message
    */
  override def channelRead0(ctx: ChannelHandlerContext, msg: TransactionStateMessage): Unit = {
    logger.debug(s"[READ PARTITION_${msg.partition}] ts=${msg.transactionUUID.timestamp()} ttl=${msg.ttl} status=${msg.status}")
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
  * Decoder to convert string to message
  */
class ProducerTopicMessageDecoder extends MessageToMessageDecoder[String] {
  val logger = LoggerFactory.getLogger(this.getClass)

  override def decode(ctx: ChannelHandlerContext, msg: String, out: util.List[AnyRef]): Unit = {
    try {
      if (msg != null)
        out.add(ProtocolMessageSerializer.deserialize[TransactionStateMessage](msg))
    }
    catch {
      case e: ProtocolMessageSerializerException =>
        logger.warn(s"TStreams Serializer Exception: ${e.getMessage}")
    }
  }
}
