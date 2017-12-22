package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.netty.RequestMessage
import com.bwsw.tstreamstransactionserver.netty.server.authService.AuthService
import com.bwsw.tstreamstransactionserver.netty.server.transportService.TransportValidator
import io.netty.channel.ChannelHandlerContext

object RequestRouter {
  final def handlerId(clientRequestHandler: ClientRequestHandler): (Byte, RequestHandler) = {
    val id = clientRequestHandler.id
    id -> clientRequestHandler
  }

  final def handlerAuthData(clientRequestHandler: ClientRequestHandler)
                           (implicit
                            authService: AuthService,
                            transportValidator: TransportValidator): (Byte, RequestHandler) = {
    val id = clientRequestHandler.id
    id -> new AuthHandler(
      new DataPackageSizeValidationHandler(
        clientRequestHandler,
        transportValidator
      ),
      authService
    )
  }

  final def handlerAuthMetadata(clientRequestHandler: ClientRequestHandler)
                               (implicit
                                authService: AuthService,
                                transportValidator: TransportValidator): (Byte, RequestHandler) = {
    val id = clientRequestHandler.id
    id -> new AuthHandler(
      new MetadataPackageSizeValidationHandler(
        clientRequestHandler,
        transportValidator
      ),
      authService
    )
  }

  final def handlerAuth(clientRequestHandler: ClientRequestHandler)
                       (implicit
                        authService: AuthService): (Byte, RequestHandler) = {
    val id = clientRequestHandler.id
    id -> new AuthHandler(
      clientRequestHandler,
      authService
    )
  }
}

trait RequestRouter {
  def route(message: RequestMessage,
            ctx: ChannelHandlerContext): Unit
}
