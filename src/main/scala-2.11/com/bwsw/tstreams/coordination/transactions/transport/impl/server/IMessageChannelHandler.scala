package com.bwsw.tstreams.coordination.transactions.transport.impl.server

import java.util

import com.bwsw.tstreams.common.serializer.TStreamsSerializer
import com.bwsw.tstreams.coordination.transactions.messages.IMessage
import com.bwsw.tstreams.coordination.transactions.transport.impl.server.actors.IMessageListenerManager
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.JsonMappingException
import io.netty.channel._
import io.netty.handler.codec.{MessageToMessageDecoder, MessageToMessageEncoder}
import io.netty.util.ReferenceCountUtil
import org.slf4j.LoggerFactory

/**
 * Handler for managing new connections for [[IMessageListener]]]
 */
@ChannelHandler.Sharable
class IMessageServerChannelHandler(manager : IMessageListenerManager) extends SimpleChannelInboundHandler[IMessage] {

  /**
   * Triggered on new [[IMessage]]]
   * @param ctx Netty ctx
   * @param msg Received msg
   */
  override def channelRead0(ctx: ChannelHandlerContext, msg: IMessage): Unit = {
    val address = msg.senderID
    val id = ctx.channel().id()
    val channel = ctx.channel()
    manager.channelRead(address, id, channel, msg)
    ReferenceCountUtil.release(msg)
  }

  /**
   * Triggered on disconnect
   * @param ctx Netty ctx
   */
  override def channelInactive(ctx: ChannelHandlerContext) : Unit = {
    val id = ctx.channel().id()
    manager.channelInactive(id)
  }

  /**
   * Triggered on exceptions
   * @param ctx Netty ctx
   * @param cause Cause of exception
   */
  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) = {
    cause.printStackTrace()
    ctx.close()
  }
}

/**
 * Decoder [[java.lang.String]]] to [[IMessage]]]
 */
class IMessageDecoder extends MessageToMessageDecoder[String]{
  val serializer = new TStreamsSerializer
  private val logger = LoggerFactory.getLogger(this.getClass)

  override def decode(ctx: ChannelHandlerContext, msg: String, out: util.List[AnyRef]): Unit = {
    try {
      if (msg != null)
        out.add(serializer.deserialize[IMessage](msg))
    }
    catch {
      case e @ (_: JsonParseException | _: JsonMappingException) =>
        logger.warn(s"exception occured : ${e.getMessage}")
    }
  }
}

/**
 * Encoder [[IMessage]]] to [[java.lang.String]]]
 */
class IMessageEncoder extends MessageToMessageEncoder[IMessage]{
  val serializer = new TStreamsSerializer

  override def encode(ctx: ChannelHandlerContext, msg: IMessage, out: util.List[AnyRef]): Unit = {
    out.add(serializer.serialize(msg) + "\n")
  }
}