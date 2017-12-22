package com.bwsw.tstreamstransactionserver.netty.server.multiNode.cg

import com.bwsw.tstreamstransactionserver.netty.RequestMessage
import com.bwsw.tstreamstransactionserver.netty.server.authService.AuthService
import com.bwsw.tstreamstransactionserver.netty.server.handler.RequestRouter.{handlerAuthMetadata, handlerId}
import com.bwsw.tstreamstransactionserver.netty.server.handler.auth.{AuthenticateHandler, IsValidHandler}
import com.bwsw.tstreamstransactionserver.netty.server.handler.transport.GetMaxPackagesSizesHandler
import com.bwsw.tstreamstransactionserver.netty.server.handler.{RequestHandler, RequestRouter}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.BookkeeperMaster
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.metadata.PutTransactionsHandler
import com.bwsw.tstreamstransactionserver.netty.server.transportService.TransportValidator
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.{AuthenticationOptions, TransportOptions}
import io.netty.channel.ChannelHandlerContext

import scala.collection.Searching.{Found, _}
import scala.concurrent.ExecutionContext

class CheckpointGroupHandlerRouter(checkpointMaster: BookkeeperMaster,
                                   commitLogContext: ExecutionContext,
                                   packageTransmissionOpts: TransportOptions,
                                   authOptions: AuthenticationOptions)
  extends RequestRouter {

  private implicit val authService =
    new AuthService(authOptions)

  private implicit val transportValidator =
    new TransportValidator(packageTransmissionOpts)


  private val (handlersIDs: Array[Byte], handlers: Array[RequestHandler]) = Array(
    handlerAuthMetadata(new PutTransactionsHandler(
      checkpointMaster,
      commitLogContext
    )),

    handlerId(new AuthenticateHandler(
      authService
    )),
    handlerId(new IsValidHandler(
      authService
    )),

    handlerId(new GetMaxPackagesSizesHandler(
      packageTransmissionOpts
    ))
  ).sortBy(_._1).unzip



  override def route(message: RequestMessage,
                     ctx: ChannelHandlerContext): Unit = {
    handlersIDs.search(message.methodId) match {
      case Found(index) =>
        val handler = handlers(index)
        handler.handle(message, ctx, None)
      case _ =>
      //        throw new IllegalArgumentException(s"Not implemented method that has id: ${message.methodId}")
    }
  }
}
