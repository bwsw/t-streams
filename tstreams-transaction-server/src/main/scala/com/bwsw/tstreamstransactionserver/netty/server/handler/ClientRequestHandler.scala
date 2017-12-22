package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.netty.{RequestMessage, ResponseMessage}
import com.bwsw.tstreamstransactionserver.tracing.ServerTracer.tracer
import io.netty.channel.ChannelHandlerContext
import org.slf4j.{Logger, LoggerFactory}

abstract class ClientRequestHandler(val id: Byte,
                                    val name: String)
  extends RequestHandler
    with Ordered[ClientRequestHandler] {
  protected final val logger: Logger =
    LoggerFactory.getLogger(this.getClass)

  override final def compare(that: ClientRequestHandler): Int = {
    java.lang.Byte.compare(this.id, that.id)
  }

  def createErrorResponse(message: String): Array[Byte]

  protected final def logSuccessfulProcession(method: String,
                                              message: RequestMessage,
                                              ctx: ChannelHandlerContext): Unit =
    if (logger.isDebugEnabled)
      logger.debug(s"Client [${ctx.channel().remoteAddress().toString}, request id ${message.id}]: " +
        s"$method is successfully processed!")

  protected final def logUnsuccessfulProcessing(method: String,
                                                error: Throwable,
                                                message: RequestMessage,
                                                ctx: ChannelHandlerContext): Unit =
    if (logger.isDebugEnabled)
      logger.debug(s"Client [${ctx.channel().remoteAddress().toString}, request id ${message.id}]: " +
        s"$method is failed while processing!", error)

  protected final def sendResponse(message: RequestMessage,
                                   response: Array[Byte],
                                   ctx: ChannelHandlerContext): Unit = {
    val responseMessage = ResponseMessage(message.id, response)
    val binaryResponse = responseMessage.toByteBuf(ctx.alloc())
    val channel = ctx.channel()
    if (channel.isActive) {
      tracer.serverSend(message)
      channel.eventLoop().execute(() =>
        ctx.writeAndFlush(binaryResponse, ctx.voidPromise()))
    }
  }
}
