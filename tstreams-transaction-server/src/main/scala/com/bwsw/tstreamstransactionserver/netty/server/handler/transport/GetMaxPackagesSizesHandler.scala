package com.bwsw.tstreamstransactionserver.netty.server.handler.transport

import com.bwsw.tstreamstransactionserver.netty.server.handler.SyncReadHandler
import com.bwsw.tstreamstransactionserver.netty.server.handler.transport.GetMaxPackagesSizesHandler._
import com.bwsw.tstreamstransactionserver.netty.{Protocol, RequestMessage}
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.TransportOptions
import com.bwsw.tstreamstransactionserver.rpc.{TransactionService, TransportOptionsInfo}
import io.netty.channel.ChannelHandlerContext


private object GetMaxPackagesSizesHandler {
  val descriptor = Protocol.GetMaxPackagesSizes
}

class GetMaxPackagesSizesHandler(packageTransmissionOpts: TransportOptions)
  extends SyncReadHandler(
    descriptor.methodID,
    descriptor.name
  ) {

  override protected def getResponse(message: RequestMessage,
                                     ctx: ChannelHandlerContext,
                                     error: Option[Throwable]): Array[Byte] = {
    scala.util.Try(process(message.body)) match {
      case scala.util.Success(result) =>
        val response = descriptor.encodeResponse(
          TransactionService.GetMaxPackagesSizes.Result(Some(result))
        )
        response
      case scala.util.Failure(throwable) =>
        val response = createErrorResponse(throwable.getMessage)
        response
    }
  }

  private def process(requestBody: Array[Byte]) = {
    val response = TransportOptionsInfo(
      packageTransmissionOpts.maxMetadataPackageSize,
      packageTransmissionOpts.maxDataPackageSize
    )
    response
  }

  override def createErrorResponse(message: String): Array[Byte] = {
    throw new UnsupportedOperationException("IsValid method doesn't imply error at all!")
  }
}
