package com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.data

import com.bwsw.tstreamstransactionserver.netty.{Protocol, RequestMessage}
import com.bwsw.tstreamstransactionserver.netty.server.batch.Frame
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.BookkeeperMaster
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.Record
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.MultiNodePredefinedContextHandler
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.data.PutProducerStateWithDataHandler._
import com.bwsw.tstreamstransactionserver.rpc.{ServerException, TransactionService}
import org.apache.bookkeeper.client.BKException.Code
import org.apache.bookkeeper.client.{AsyncCallback, LedgerHandle}

import scala.concurrent.{ExecutionContext, Future, Promise}

private object PutProducerStateWithDataHandler {
  val descriptor = Protocol.PutProducerStateWithData

  val isPuttedResponse: Array[Byte] = descriptor.encodeResponse(
    TransactionService.PutProducerStateWithData.Result(Some(true))
  )
  val isNotPuttedResponse: Array[Byte] = descriptor.encodeResponse(
    TransactionService.PutProducerStateWithData.Result(Some(false))
  )
}

class PutProducerStateWithDataHandler(bookkeeperMaster: BookkeeperMaster,
                                      context: ExecutionContext)
  extends MultiNodePredefinedContextHandler(
    descriptor.methodID,
    descriptor.name,
    context) {

  private val callback = new AsyncCallback.AddCallback {
    override def addComplete(bkCode: Int,
                             ledgerHandle: LedgerHandle,
                             entryId: Long,
                             obj: scala.Any): Unit = {
      val promise = obj.asInstanceOf[Promise[Array[Byte]]]
      if (Code.OK == bkCode)
        promise.success(isPuttedResponse)
      else
        promise.success(isNotPuttedResponse)

    }
  }
  private def process(requestBody: Array[Byte]) = {

    val promise = Promise[Array[Byte]]()
    Future {
      bookkeeperMaster.doOperationWithCurrentWriteLedger(ledgerHandlerOrError =>
        ledgerHandlerOrError.foreach { ledgerHandler =>
          val record = new Record(
            Frame.PutProducerStateWithDataType.id.toByte,
            System.currentTimeMillis(),
            requestBody
          ).toByteArray

          ledgerHandler.asyncAddEntry(record, callback, promise)
        }
      )
    }(context)
      .flatMap(_ => promise.future)(context)
  }

  override protected def fireAndForget(message: RequestMessage): Unit = {
    process(message.body)
  }

  override protected def getResponse(message: RequestMessage): Future[Array[Byte]] = {
    process(message.body)
  }

  override def createErrorResponse(message: String): Array[Byte] = {
    descriptor.encodeResponse(
      TransactionService.PutProducerStateWithData.Result(
        None,
        Some(ServerException(message)
        )
      )
    )
  }

}
