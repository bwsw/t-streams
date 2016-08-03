package com.bwsw.tstreams.coordination.transactions.transport.impl.server

import java.util

import com.bwsw.tstreams.common.serializer.TStreamsSerializer
import com.bwsw.tstreams.common.serializer.TStreamsSerializer.TStreamsSerializerException
import com.bwsw.tstreams.coordination.transactions.messages.IMessage
import io.netty.channel._
import io.netty.handler.codec.{MessageToMessageDecoder, MessageToMessageEncoder}
import io.netty.util.ReferenceCountUtil
import org.slf4j.LoggerFactory

/**
  * Handler for managing new connections for [[TransactionStateMessageListener]]]
  */
@ChannelHandler.Sharable
class TransactionStateMessageServerChannelHandler(manager: TransactionStateMessageListenerManager) extends SimpleChannelInboundHandler[IMessage] {

  /**
    * Triggered on new [[IMessage]]]
    *
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
    *
    * @param ctx Netty ctx
    */
  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    val id = ctx.channel().id()
    manager.channelInactive(id)
  }

  /**
    * Triggered on exceptions
    *
    * @param ctx   Netty ctx
    * @param cause Cause of exception
    */
  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) = {
    println(s"IMessageServer exception : ${cause.getMessage}")
    //    ctx.close()
  }
}

/**
  * Decoder [[java.lang.String]]] to [[IMessage]]]
  */
class TransactionStateMessageDecoder extends MessageToMessageDecoder[String] {
  val serializer = new TStreamsSerializer
  private val logger = LoggerFactory.getLogger(this.getClass)

  override def decode(ctx: ChannelHandlerContext, msg: String, out: util.List[AnyRef]): Unit = {
    try {
      if (msg != null)
        out.add(serializer.deserialize[IMessage](msg))
    }
    catch {
      case e: TStreamsSerializerException =>
        logger.warn(s"TStreamsSerializerException : ${e.getMessage}")
    }
  }
}

/**
  * Encoder [[IMessage]]] to [[java.lang.String]]]
  */
class TransactionStateMessageEncoder extends MessageToMessageEncoder[IMessage] {
  val serializer = new TStreamsSerializer

  override def encode(ctx: ChannelHandlerContext, msg: IMessage, out: util.List[AnyRef]): Unit = {
    out.add(serializer.serialize(msg) + "\n")
  }
}