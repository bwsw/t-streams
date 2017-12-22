package com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.metadata

import com.bwsw.tstreamstransactionserver.netty.server.batch.Frame
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.BookkeeperMaster
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.Record
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.MultiNodeArgsDependentContextHandler
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.metadata.OpenTransactionHandler._
import com.bwsw.tstreamstransactionserver.netty.server.subscriber.OpenedTransactionNotifier
import com.bwsw.tstreamstransactionserver.netty.server.{OrderedExecutionContextPool, TransactionServer}
import com.bwsw.tstreamstransactionserver.netty.{Protocol, RequestMessage}
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.AuthenticationOptions
import com.bwsw.tstreamstransactionserver.protocol.TransactionState
import com.bwsw.tstreamstransactionserver.rpc._
import com.bwsw.tstreamstransactionserver.tracing.ServerTracer.tracer
import io.netty.channel.ChannelHandlerContext
import org.apache.bookkeeper.client.BKException.Code
import org.apache.bookkeeper.client.{AsyncCallback, BKException, LedgerHandle}

import scala.concurrent.{ExecutionContext, Future, Promise}

private object OpenTransactionHandler {
  val descriptor = Protocol.OpenTransaction
}

class OpenTransactionHandler(server: TransactionServer,
                             bookkeeperMaster: BookkeeperMaster,
                             notifier: OpenedTransactionNotifier,
                             authOptions: AuthenticationOptions,
                             orderedExecutionPool: OrderedExecutionContextPool,
                             context: ExecutionContext)
  extends MultiNodeArgsDependentContextHandler(
    descriptor.methodID,
    descriptor.name,
    orderedExecutionPool) {

  private val fireAndForgetLedger = getClass.getName + ".fireAndForget.ledgerHandler.asyncAddEntry"
  private val getResponseLedger = getClass.getName + ".getResponse.ledgerHandler.asyncAddEntry"

  private trait Callback


  private class ReplyCallback(stream: Int,
                              partition: Int,
                              transactionId: Long,
                              ttlMs: Long,
                              message: RequestMessage,
                              ctx: ChannelHandlerContext)
    extends AsyncCallback.AddCallback {
    override def addComplete(bkCode: Int,
                             ledgerHandle: LedgerHandle,
                             entryId: Long,
                             obj: scala.Any): Unit = {
      tracer.finish(message, getResponseLedger)
      tracer.withTracing(message, getClass.getName + ".addComplete") {
        val promise = obj.asInstanceOf[Promise[Boolean]]
        if (Code.OK == bkCode) {

          val response = descriptor.encodeResponse(
            TransactionService.OpenTransaction.Result(
              Some(transactionId)
            )
          )

          sendResponse(message, response, ctx)

          notifier.notifySubscribers(
            stream,
            partition,
            transactionId,
            0,
            TransactionState.Status.Opened,
            ttlMs,
            authOptions.key,
            isNotReliable = false,
            message
          )
          promise.success(true)
        }
        else {
          promise.failure(BKException.create(bkCode).fillInStackTrace())
        }
      }
    }
  }

  private class FireAndForgetCallback(stream: Int,
                                      partition: Int,
                                      transactionId: Long,
                                      ttlMs: Long,
                                      message: RequestMessage)
    extends AsyncCallback.AddCallback {
    override def addComplete(bkCode: Int,
                             ledgerHandle: LedgerHandle,
                             entryId: Long,
                             obj: scala.Any): Unit = {
      tracer.finish(message, fireAndForgetLedger)
      tracer.withTracing(message, getClass.getName + ".addComplete") {
        val promise = obj.asInstanceOf[Promise[Boolean]]
        if (Code.OK == bkCode) {
          notifier.notifySubscribers(
            stream,
            partition,
            transactionId,
            count = 0,
            TransactionState.Status.Opened,
            ttlMs,
            authOptions.key,
            isNotReliable = true,
            message
          )
          promise.success(true)
        }
        else {
          promise.failure(BKException.create(bkCode).fillInStackTrace())
        }
      }
    }
  }

  override protected def fireAndForget(message: RequestMessage): Unit = {
    tracer.withTracing(message, getClass.getName + ".fireAndForget") {
      val args = descriptor.decodeRequest(message.body)
      val context = orderedExecutionPool.pool(args.streamID, args.partition)

      def helper(): Future[Boolean] = {
        val promise = Promise[Boolean]()
        Future {
          tracer.withTracing(message, getClass.getName + ".fireAndForget.Future") {
            bookkeeperMaster.doOperationWithCurrentWriteLedger {
              case Left(throwable) =>
                promise.failure(throwable)

              case Right(ledgerHandler) =>
                val transactionID = server.getTransactionID

                val txn = Transaction(Some(
                  ProducerTransaction(
                    args.streamID,
                    args.partition,
                    transactionID,
                    TransactionStates.Opened,
                    quantity = 0,
                    ttl = args.transactionTTLMs
                  )), None
                )

                val binaryTransaction = Protocol.PutTransaction.encodeRequest(
                  TransactionService.PutTransaction.Args(txn)
                )

                val callback = new FireAndForgetCallback(
                  args.streamID,
                  args.partition,
                  transactionID,
                  args.transactionTTLMs,
                  message)

                val record = new Record(
                  Frame.PutTransactionType.id.toByte,
                  System.currentTimeMillis(),
                  binaryTransaction
                ).toByteArray

                tracer.invoke(message, fireAndForgetLedger)
                ledgerHandler.asyncAddEntry(record, callback, promise)
            }
          }
        }(context)
        promise.future
      }

      helper()
    }
  }

  override protected def getResponse(message: RequestMessage,
                                     ctx: ChannelHandlerContext): (Future[_], ExecutionContext) = {
    tracer.withTracing(message, getClass.getName + ".getResponse") {
      val args = descriptor.decodeRequest(message.body)
      val context = orderedExecutionPool.pool(args.streamID, args.partition)

      def helper(): Future[Boolean] = {
        val promise = Promise[Boolean]()
        Future {
          tracer.withTracing(message, getClass.getName + ".getResponse.Future") {
            bookkeeperMaster.doOperationWithCurrentWriteLedger {
              case Left(throwable) =>
                promise.failure(throwable)

              case Right(ledgerHandler) =>
                val transactionId = server.getTransactionID

                val txn = Transaction(Some(
                  ProducerTransaction(
                    args.streamID,
                    args.partition,
                    transactionId,
                    TransactionStates.Opened,
                    quantity = 0,
                    ttl = args.transactionTTLMs
                  )), None
                )

                val binaryTransaction = Protocol.PutTransaction.encodeRequest(
                  TransactionService.PutTransaction.Args(txn)
                )

                val callback = new ReplyCallback(
                  args.streamID,
                  args.partition,
                  transactionId,
                  args.transactionTTLMs,
                  message,
                  ctx
                )

                val record = new Record(
                  Frame.PutTransactionType.id.toByte,
                  System.currentTimeMillis(),
                  binaryTransaction
                ).toByteArray

                tracer.invoke(message, getResponseLedger)
                ledgerHandler.asyncAddEntry(record, callback, promise)
            }
          }
        }(context)
        promise.future
      }

      (helper(), context)
    }
  }

  override def createErrorResponse(message: String): Array[Byte] = {
    descriptor.encodeResponse(
      TransactionService.OpenTransaction.Result(
        None,
        Some(ServerException(message)
        )
      )
    )
  }
}
