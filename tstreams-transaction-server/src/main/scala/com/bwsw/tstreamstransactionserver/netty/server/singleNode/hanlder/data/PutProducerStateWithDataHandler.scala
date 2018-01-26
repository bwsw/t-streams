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
      Frame.PutTransactionType,
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