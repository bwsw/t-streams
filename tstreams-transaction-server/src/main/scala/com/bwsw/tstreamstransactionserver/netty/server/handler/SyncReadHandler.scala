package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.netty.RequestMessage
import com.bwsw.tstreamstransactionserver.tracing.ServerTracer.tracer
import io.netty.channel.ChannelHandlerContext


abstract class SyncReadHandler(override final val id: Byte,
                               override final val name: String)
  extends ClientRequestHandler(id, name) {


  override final def handle(message: RequestMessage,
                            ctx: ChannelHandlerContext,
                            acc: Option[Throwable]): Unit = {
    tracer.withTracing(message, name = getClass.getName + ".handle") {
      if (!message.isFireAndForgetMethod) {
        val response = getResponse(message, ctx, acc)
        sendResponse(message, response, ctx)
      }
    }
  }

  protected def getResponse(message: RequestMessage,
                            ctx: ChannelHandlerContext,
                            error: Option[Throwable]): Array[Byte]
}
