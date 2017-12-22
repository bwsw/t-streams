package com.bwsw.tstreamstransactionserver.netty.server.singleNode.hanlder.data

import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.batch.Frame
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.ScheduledCommitLog
import com.bwsw.tstreamstransactionserver.netty.server.handler.PredefinedContextHandler
import com.bwsw.tstreamstransactionserver.netty.{Protocol, RequestMessage}
import com.bwsw.tstreamstransactionserver.rpc._
import io.netty.channel.ChannelHandlerContext

import scala.concurrent.ExecutionContext

import com.bwsw.tstreamstransactionserver.netty.server.singleNode.hanlder.data.PutProducerStateWithDataHandler._

private object PutProducerStateWithDataHandler {
  val descriptor = Protocol.PutProducerStateWithData
}

class PutProducerStateWithDataHandler(server: TransactionServer,
                                      scheduledCommitLog: ScheduledCommitLog,
                                      context: ExecutionContext)
  extends PredefinedContextHandler(
    descriptor.methodID,
    descriptor.name,
    context) {

  override def createErrorResponse(message: String): Array[Byte] = {
    descriptor.encodeResponse(
      TransactionService.PutProducerStateWithData.Result(
        None,
        Some(ServerException(message)
        )
      )
    )
  }

  override protected def fireAndForget(message: RequestMessage): Unit = {
    process(message.body)
  }

  private def process(requestBody: Array[Byte]): Boolean = {
    val transactionAndData = descriptor.decodeRequest(requestBody)
    val txn = transactionAndData.transaction
    val data = transactionAndData.data
    val from = transactionAndData.from

    server.putTransactionData(
      txn.stream,
      txn.partition,
      txn.transactionID,
      data,
      from
    )

    val transaction = Transaction(
      Some(
        ProducerTransaction(
          txn.stream,
          txn.partition,
          txn.transactionID,
          txn.state,
          txn.quantity,
          txn.ttl
        )),
      None
    )

    val binaryTransaction = Protocol.PutTransaction.encodeRequest(
      TransactionService.PutTransaction.Args(transaction)
    )

    scheduledCommitLog.putData(
      Frame.PutTransactionType.id.toByte,
      binaryTransaction
    )
  }

  override protected def getResponse(message: RequestMessage,
                                     ctx: ChannelHandlerContext): Array[Byte] = {
    val response = descriptor.encodeResponse(
      TransactionService.PutProducerStateWithData.Result(
        Some(process(message.body))
      )
    )
    response
  }
}