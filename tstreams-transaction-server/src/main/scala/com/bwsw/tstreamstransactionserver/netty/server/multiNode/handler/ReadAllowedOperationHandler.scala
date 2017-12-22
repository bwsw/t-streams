package com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler

import com.bwsw.tstreamstransactionserver.netty.RequestMessage
import com.bwsw.tstreamstransactionserver.netty.server.handler.{IntermediateRequestHandler, RequestHandler}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.BookkeeperWriter
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.commitLogService.CommitLogService
import com.bwsw.tstreamstransactionserver.tracing.ServerTracer.tracer
import io.netty.channel.ChannelHandlerContext
import org.apache.curator.framework.recipes.leader.LeaderLatchListener


object ReadAllowedOperationHandler {
  val NO_LEDGER_PROCESSED: Long = -1L
}

class ReadAllowedOperationHandler(nextHandler: RequestHandler,
                                  commitLogService: CommitLogService,
                                  bookkeeperWriter: BookkeeperWriter)
  extends IntermediateRequestHandler(nextHandler)
    with LeaderLatchListener {

  @volatile private var ledgerToStartToReadFrom =
    ReadAllowedOperationHandler.NO_LEDGER_PROCESSED

  override def isLeader() = {
    ledgerToStartToReadFrom =
      bookkeeperWriter.getLastConstructedLedger
  }

  override def notLeader() = {
    ledgerToStartToReadFrom =
      ReadAllowedOperationHandler.NO_LEDGER_PROCESSED
  }


  def canPerformReadOperation: Boolean = {
    val ledgersIds =
      commitLogService.getMinMaxLedgersIds

    ledgersIds.minLedgerId >= ledgerToStartToReadFrom
  }

  override def handle(message: RequestMessage,
                      ctx: ChannelHandlerContext,
                      error: Option[Throwable]): Unit = {
    tracer.withTracing(message, name = getClass.getName + ".handle") {
      if (canPerformReadOperation)
        nextHandler.handle(message, ctx, error)
    }
  }
}
