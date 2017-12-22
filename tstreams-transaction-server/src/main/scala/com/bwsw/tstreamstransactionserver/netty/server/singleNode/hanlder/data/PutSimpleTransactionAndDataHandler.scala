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
package com.bwsw.tstreamstransactionserver.netty.server.singleNode.hanlder.data

import com.bwsw.tstreamstransactionserver.netty.server.batch.Frame
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.ScheduledCommitLog
import com.bwsw.tstreamstransactionserver.netty.server.handler.ArgsDependentContextHandler
import com.bwsw.tstreamstransactionserver.netty.server.subscriber.OpenedTransactionNotifier
import com.bwsw.tstreamstransactionserver.netty.server.{OrderedExecutionContextPool, TransactionServer}
import com.bwsw.tstreamstransactionserver.netty.{Protocol, RequestMessage}
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.AuthenticationOptions
import com.bwsw.tstreamstransactionserver.protocol.TransactionState
import com.bwsw.tstreamstransactionserver.rpc.TransactionService.PutSimpleTransactionAndData
import com.bwsw.tstreamstransactionserver.rpc._
import io.netty.channel.ChannelHandlerContext

import scala.concurrent.{ExecutionContext, Future}
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.hanlder.data.PutSimpleTransactionAndDataHandler._

private object PutSimpleTransactionAndDataHandler {
  val descriptor = Protocol.PutSimpleTransactionAndData
}

class PutSimpleTransactionAndDataHandler(server: TransactionServer,
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
      TransactionService.PutSimpleTransactionAndData.Result(
        None,
        Some(ServerException(message)
        )
      )
    )
  }

  override protected def fireAndForget(message: RequestMessage): Unit = {
    val args = descriptor.decodeRequest(message.body)
    val context = getContext(args.streamID, args.partition)
    Future {
      val transactionID =
        server.getTransactionID

      process(args, transactionID)

      notifier.notifySubscribers(
        args.streamID,
        args.partition,
        transactionID,
        args.data.size,
        TransactionState.Status.Instant,
        Long.MaxValue,
        authOptions.key,
        isNotReliable = true
      )
    }(context)
  }

  private def process(txn: PutSimpleTransactionAndData.Args,
                      transactionID: Long) = {

    server.putTransactionData(
      txn.streamID,
      txn.partition,
      transactionID,
      txn.data,
      0
    )

    val transactions = collection.immutable.Seq(
      Transaction(Some(
        ProducerTransaction(
          txn.streamID,
          txn.partition,
          transactionID,
          TransactionStates.Opened,
          txn.data.size, 3000L
        )), None
      ),
      Transaction(Some(
        ProducerTransaction(
          txn.streamID,
          txn.partition,
          transactionID,
          TransactionStates.Checkpointed,
          txn.data.size,
          Long.MaxValue)), None
      )
    )

    val messageForPutTransactions =
      Protocol.PutTransactions.encodeRequest(
        TransactionService.PutTransactions.Args(transactions)
      )

    scheduledCommitLog.putData(
      Frame.PutTransactionsType.id.toByte,
      messageForPutTransactions
    )
  }

  override protected def getResponse(message: RequestMessage,
                                     ctx: ChannelHandlerContext): (Future[_], ExecutionContext) = {
    val args = descriptor.decodeRequest(message.body)
    val context = orderedExecutionPool.pool(args.streamID, args.partition)
    val result = Future {
      val transactionID =
        server.getTransactionID

      process(args, transactionID)

      val response = descriptor.encodeResponse(
        TransactionService.PutSimpleTransactionAndData.Result(
          Some(transactionID)
        )
      )

      sendResponse(message, response, ctx)

      notifier.notifySubscribers(
        args.streamID,
        args.partition,
        transactionID,
        args.data.size,
        TransactionState.Status.Instant,
        Long.MaxValue,
        authOptions.key,
        isNotReliable = false
      )
    }(context)
    (result, context)
  }
}
