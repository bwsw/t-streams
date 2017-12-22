package com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.commitLog

import com.bwsw.tstreamstransactionserver.netty.{Protocol, RequestMessage}
import com.bwsw.tstreamstransactionserver.netty.server.handler.PredefinedContextHandler
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.BookkeeperWriter
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.commitLog.GetCommitLogOffsetsHandler._
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.commitLogService.CommitLogService
import com.bwsw.tstreamstransactionserver.rpc.{CommitLogInfo, ServerException, TransactionService}
import io.netty.channel.ChannelHandlerContext

import scala.concurrent.ExecutionContext

private object GetCommitLogOffsetsHandler {
  val descriptor = Protocol.GetCommitLogOffsets
}

class GetCommitLogOffsetsHandler(commitLogService: CommitLogService,
                                 bookkeeperWriter: BookkeeperWriter,
                                 context: ExecutionContext)
  extends PredefinedContextHandler(
    descriptor.methodID,
    descriptor.name,
    context) {

  override def createErrorResponse(message: String): Array[Byte] = {
    descriptor.encodeResponse(
      TransactionService.GetCommitLogOffsets.Result(
        None,
        Some(ServerException(message))
      )
    )
  }

  override protected def fireAndForget(message: RequestMessage): Unit = {}

  override protected def getResponse(message: RequestMessage,
                                     ctx: ChannelHandlerContext): Array[Byte] = {
    val response = descriptor.encodeResponse(
      TransactionService.GetCommitLogOffsets.Result(
        Some(process(message.body))
      )
    )
    response
  }

  private def process(requestBody: Array[Byte]) = {
    val ledgers =
      commitLogService.getMinMaxLedgersIds

    CommitLogInfo(
      ledgers.minLedgerId,
      bookkeeperWriter.getLastConstructedLedger
    )
  }
}
