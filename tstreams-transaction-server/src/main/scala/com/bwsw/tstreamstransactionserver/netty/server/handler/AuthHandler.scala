package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException
import com.bwsw.tstreamstransactionserver.netty.RequestMessage
import com.bwsw.tstreamstransactionserver.netty.server.authService.AuthService
import com.bwsw.tstreamstransactionserver.tracing.ServerTracer.tracer
import io.netty.channel.ChannelHandlerContext
import org.slf4j.{Logger, LoggerFactory}

class AuthHandler(nextHandler: RequestHandler,
                  authService: AuthService)
  extends IntermediateRequestHandler(nextHandler) {
  private val logger: Logger =
    LoggerFactory.getLogger(this.getClass)

  override def handle(message: RequestMessage,
                      ctx: ChannelHandlerContext,
                      acc: Option[Throwable]): Unit = {
    tracer.withTracing(message, name = getClass.getName + ".handle") {
      if (acc.isDefined) {
        if (message.isFireAndForgetMethod) {
          if (logger.isDebugEnabled())
            logger.debug(s"Client [${ctx.channel().remoteAddress().toString}, " +
              s"request id: ${message.id}]", acc.get)
        }
        else {
          nextHandler.handle(message, ctx, acc)
        }
      } else {
        val isValid = authService.isValid(message.token)
        if (isValid)
          nextHandler.handle(message, ctx, acc)
        else {
          if (logger.isDebugEnabled())
            logger.debug(s"Client [${ctx.channel().remoteAddress().toString}, request id: ${message.id}] " +
              s"Token ${message.token} is not valid")
          if (!message.isFireAndForgetMethod) {
            nextHandler.handle(
              message,
              ctx,
              Some(new TokenInvalidException)
            )
          }
        }
      }
    }
  }
}
