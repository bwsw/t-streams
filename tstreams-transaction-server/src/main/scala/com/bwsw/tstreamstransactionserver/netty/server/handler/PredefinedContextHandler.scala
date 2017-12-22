package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.netty.RequestMessage
import com.bwsw.tstreamstransactionserver.tracing.ServerTracer.tracer
import io.netty.channel.ChannelHandlerContext

import scala.concurrent.{ExecutionContext, Future}

abstract class PredefinedContextHandler(override final val id: Byte,
                                        override final val name: String,
                                        context: ExecutionContext)
  extends ClientRequestHandler(id, name) {

  override final def handle(message: RequestMessage,
                            ctx: ChannelHandlerContext,
                            acc: Option[Throwable]): Unit = {
    tracer.withTracing(message, name = getClass.getName + ".handle") {
      if (message.isFireAndForgetMethod)
        handleFireAndForgetRequest(message, ctx, acc)
      else
        handleRequest(message, ctx, acc)
    }
  }

  private def handleFireAndForgetRequest(message: RequestMessage,
                                         ctx: ChannelHandlerContext,
                                         acc: Option[Throwable]) = {
    if (acc.isEmpty) {
      Future(fireAndForget(message))(context)
    } else {
      logUnsuccessfulProcessing(
        name,
        acc.get,
        message,
        ctx
      )
    }
  }

  private def handleRequest(message: RequestMessage,
                            ctx: ChannelHandlerContext,
                            error: Option[Throwable]) = {
    if (error.isEmpty) {
      Future {
        val response = getResponse(message, ctx)
        sendResponse(message, response, ctx)
      }(context)
        .recover { case throwable =>
          logUnsuccessfulProcessing(name, throwable, message, ctx)
          val response = createErrorResponse(throwable.getMessage)
          sendResponse(message, response, ctx)
        }(context)
    } else {
      val throwable = error.get
      logUnsuccessfulProcessing(name, throwable, message, ctx)
      val response = createErrorResponse(throwable.getMessage)
      sendResponse(message, response, ctx)
    }
  }

  protected def fireAndForget(message: RequestMessage): Unit

  protected def getResponse(message: RequestMessage,
                            ctx: ChannelHandlerContext): Array[Byte]
}
