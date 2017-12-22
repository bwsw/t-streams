/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.bwsw.tstreamstransactionserver.netty.server.singleNode.hanlder.metadata

import com.bwsw.tstreamstransactionserver.netty.server.batch.Frame
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.ScheduledCommitLog
import com.bwsw.tstreamstransactionserver.netty.server.handler.ArgsDependentContextHandler
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.hanlder.metadata.OpenTransactionHandler._
import com.bwsw.tstreamstransactionserver.netty.server.subscriber.OpenedTransactionNotifier
import com.bwsw.tstreamstransactionserver.netty.server.{OrderedExecutionContextPool, TransactionServer}
import com.bwsw.tstreamstransactionserver.netty.{Protocol, RequestMessage}
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.AuthenticationOptions
import com.bwsw.tstreamstransactionserver.protocol.TransactionState
import com.bwsw.tstreamstransactionserver.rpc.TransactionService.OpenTransaction
import com.bwsw.tstreamstransactionserver.rpc._
import com.bwsw.tstreamstransactionserver.tracing.ServerTracer.tracer
import io.netty.channel.ChannelHandlerContext

import scala.concurrent.{ExecutionContext, Future}


private object OpenTransactionHandler {
  val descriptor = Protocol.OpenTransaction
}

class OpenTransactionHandler(server: TransactionServer,
                             scheduledCommitLog: ScheduledCommitLog,
                             notifier: OpenedTransactionNotifier,
                             authOptions: AuthenticationOptions,
                             orderedExecutionPool: OrderedExecutionContextPool)
  extends ArgsDependentContextHandler(
    descriptor.methodID,
    descriptor.name,
    orderedExecutionPool) {


  override def createErrorResponse(message: String): Array[Byte] = {
    descriptor.encodeResponse(
      TransactionService.OpenTransaction.Result(
        None,
        Some(ServerException(message)
        )
      )
    )
  }

  override protected def fireAndForget(message: RequestMessage): Unit = {
    tracer.withTracing(message, name = getClass.getName + ".fireAndForget") {
      val args = descriptor.decodeRequest(message.body)
      val context = getContext(args.streamID, args.partition)
      Future {
        tracer.withTracing(message, name = getClass.getName + ".fireAndForget.Future") {
          val transactionID =
            server.getTransactionID

          process(args, transactionID, message)

          notifier.notifySubscribers(
            args.streamID,
            args.partition,
            transactionID,
            count = 0,
            TransactionState.Status.Opened,
            args.transactionTTLMs,
            authOptions.key,
            isNotReliable = true,
            message
          )
        }
      }(context)
    }
  }

  override protected def getResponse(message: RequestMessage, ctx: ChannelHandlerContext): (Future[_], ExecutionContext) = {
    val result = tracer.withTracing(message, name = getClass.getName + ".getResponse") {
      val args = descriptor.decodeRequest(message.body)
      val context = orderedExecutionPool.pool(args.streamID, args.partition)
      val result = Future {
        tracer.withTracing(message, name = getClass.getName + ".getResponse.Future") {
          val transactionID =
            server.getTransactionID

          process(args, transactionID, message)

          val response = descriptor.encodeResponse(
            TransactionService.OpenTransaction.Result(
              Some(transactionID)
            )
          )

          sendResponse(message, response, ctx)

          notifier.notifySubscribers(
            args.streamID,
            args.partition,
            transactionID,
            count = 0,
            TransactionState.Status.Opened,
            args.transactionTTLMs,
            authOptions.key,
            isNotReliable = false,
            message
          )
        }
      }(context)

      (result, context)
    }

    result
  }

  private def process(args: OpenTransaction.Args,
                      transactionId: Long,
                      message: RequestMessage): Unit = {
    tracer.withTracing(message, name = getClass.getName + ".process") {
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

      scheduledCommitLog.putData(
        Frame.PutTransactionType.id.toByte,
        binaryTransaction
      )
    }
  }
}
