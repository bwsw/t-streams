package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.netty.RequestMessage
import com.bwsw.tstreamstransactionserver.netty.server.OrderedExecutionContextPool
import com.bwsw.tstreamstransactionserver.tracing.ServerTracer.tracer
import io.netty.channel.ChannelHandlerContext

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}

abstract class ArgsDependentContextHandler(override final val id: Byte,
                                           override final val name: String,
                                           orderedExecutionPool: OrderedExecutionContextPool)
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
                                         error: Option[Throwable]) = {
    if (error.isEmpty) {
      fireAndForget(message)
    } else {
      logUnsuccessfulProcessing(
        name,
        error.get,
        message,
        ctx)
    }
  }

  private def handleRequest(message: RequestMessage,
                            ctx: ChannelHandlerContext,
                            acc: Option[Throwable]) = {
    if (acc.isEmpty) {
      val (result, context) = getResponse(message, ctx)
      result.recover { case error =>
        logUnsuccessfulProcessing(name, error, message, ctx)
        val response =
          createErrorResponse(error.getMessage)
        sendResponse(message, response, ctx)
      }(context)
    } else {
      val error = acc.get
      logUnsuccessfulProcessing(name, error, message, ctx)
      val response = createErrorResponse(error.getMessage)
      sendResponse(message, response, ctx)
    }
  }

  protected def getContext(streamId: Int, partition: Int): ExecutionContextExecutorService = {
    orderedExecutionPool.pool(streamId, partition)
  }

  protected def fireAndForget(message: RequestMessage): Unit

  protected def getResponse(message: RequestMessage,
                            ctx: ChannelHandlerContext): (Future[_], ExecutionContext)
}

