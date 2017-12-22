package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException
import com.bwsw.tstreamstransactionserver.netty.RequestMessage
import com.bwsw.tstreamstransactionserver.netty.server.transportService.TransportValidator
import com.bwsw.tstreamstransactionserver.tracing.ServerTracer.tracer
import io.netty.channel.ChannelHandlerContext
import org.slf4j.{Logger, LoggerFactory}

class MetadataPackageSizeValidationHandler(nextHandler: RequestHandler,
                                           transportValidator: TransportValidator)
  extends IntermediateRequestHandler(nextHandler) {
  private val logger: Logger =
    LoggerFactory.getLogger(this.getClass)

  override def handle(message: RequestMessage,
                      ctx: ChannelHandlerContext,
                      error: Option[Throwable]): Unit = {
    tracer.withTracing(message, name = getClass.getName + ".handle") {
      if (error.isDefined) {
        if (message.isFireAndForgetMethod) {
          if (logger.isDebugEnabled())
            logger.debug(s"Client ${ctx.channel().remoteAddress().toString}", error.get)
        }
        else {
          nextHandler.handle(message, ctx, error)
        }
      }
      else {
        val isPackageTooBig = transportValidator
          .isTooBigMetadataMessage(message)

        if (isPackageTooBig)
          nextHandler.handle(
            message,
            ctx,
            Some(createError(message, ctx))
          )
        else
          nextHandler.handle(
            message,
            ctx,
            error
          )
      }
    }
  }

  private def createError(message: RequestMessage,
                          ctx: ChannelHandlerContext) = {
    new PackageTooBigException(
      s"A size of client[${ctx.channel().remoteAddress().toString}, request id: ${message.id}] request is greater " +
        s"than maxMetadataPackageSize (${transportValidator.maxMetadataPackageSize})"
    )
  }
}
