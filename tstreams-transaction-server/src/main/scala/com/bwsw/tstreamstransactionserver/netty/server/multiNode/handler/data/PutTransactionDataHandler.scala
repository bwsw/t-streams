package com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.data

import com.bwsw.tstreamstransactionserver.netty.server.batch.Frame
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.BookkeeperMaster
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.Record
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.MultiNodePredefinedContextHandler
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.data.PutTransactionDataHandler._
import com.bwsw.tstreamstransactionserver.netty.{Protocol, RequestMessage}
import com.bwsw.tstreamstransactionserver.rpc.{ServerException, TransactionService}
import com.bwsw.tstreamstransactionserver.tracing.ServerTracer.tracer
import org.apache.bookkeeper.client.BKException.Code
import org.apache.bookkeeper.client.{AsyncCallback, BKException, LedgerHandle}

import scala.concurrent.{ExecutionContext, Future, Promise}

private object PutTransactionDataHandler {
  val descriptor = Protocol.PutTransactionData

  val isPuttedResponse: Array[Byte] = descriptor.encodeResponse(
    TransactionService.PutTransactionData.Result(Some(true))
  )
  val isNotPuttedResponse: Array[Byte] = descriptor.encodeResponse(
    TransactionService.PutTransactionData.Result(Some(false))
  )
}


class PutTransactionDataHandler(bookkeeperMaster: BookkeeperMaster,
                                context: ExecutionContext)
  extends MultiNodePredefinedContextHandler(
    descriptor.methodID,
    descriptor.name,
    context) {

  private val processLedger = getClass.getName + ".process.ledgerHandler.asyncAddEntry"

  private def callback(message: RequestMessage) = new AsyncCallback.AddCallback {
    override def addComplete(bkCode: Int,
                             ledgerHandle: LedgerHandle,
                             entryId: Long,
                             obj: scala.Any): Unit = {
      tracer.finish(message, processLedger)
      tracer.withTracing(message, getClass.getName + ".addComplete") {
        val promise = obj.asInstanceOf[Promise[Array[Byte]]]
        if (Code.OK == bkCode)
          promise.success(isPuttedResponse)
        else
          promise.failure(BKException.create(bkCode).fillInStackTrace())
      }
    }
  }

  private def process(message: RequestMessage): Future[Array[Byte]] = {
    tracer.withTracing(message, getClass.getName + ".process") {
      val promise = Promise[Array[Byte]]()
      Future {
        tracer.withTracing(message, getClass.getName + ".process.Future") {
          bookkeeperMaster.doOperationWithCurrentWriteLedger {
            case Left(throwable) =>
              promise.failure(throwable)
            //          throw throwable

            case Right(ledgerHandler) =>
              val record = new Record(
                Frame.PutTransactionDataType.id.toByte,
                System.currentTimeMillis(),
                message.body
              ).toByteArray

              //          ledgerHandler.addEntry(record)
              //          isPuttedResponse
              tracer.invoke(message, processLedger)
              ledgerHandler.asyncAddEntry(record, callback(message), promise)
            //          promise
          }
        }
      }(context)

      promise.future
    }
  }


  override protected def fireAndForget(message: RequestMessage): Unit = process(message)

  override protected def getResponse(message: RequestMessage): Future[Array[Byte]] = process(message)

  override def createErrorResponse(message: String): Array[Byte] = {
    descriptor.encodeResponse(
      TransactionService.PutTransactionData.Result(None, Some(ServerException(message))))
  }
}