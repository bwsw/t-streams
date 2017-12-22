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

import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.batch.Frame
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.ScheduledCommitLog
import com.bwsw.tstreamstransactionserver.netty.server.handler.PredefinedContextHandler
import com.bwsw.tstreamstransactionserver.netty.{Protocol, RequestMessage}
import com.bwsw.tstreamstransactionserver.rpc.{ServerException, TransactionService}
import io.netty.channel.ChannelHandlerContext

import scala.concurrent.ExecutionContext
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.hanlder.metadata.PutTransactionsHandler._
import com.bwsw.tstreamstransactionserver.tracing.ServerTracer.tracer

private object PutTransactionsHandler {
  val descriptor = Protocol.PutTransactions
  val isPuttedResponse: Array[Byte] = descriptor.encodeResponse(
    TransactionService.PutTransactions.Result(Some(true))
  )
  val isNotPuttedResponse: Array[Byte] = descriptor.encodeResponse(
    TransactionService.PutTransactions.Result(Some(false))
  )
}

class PutTransactionsHandler(server: TransactionServer,
                             scheduledCommitLog: ScheduledCommitLog,
                             context: ExecutionContext)
  extends PredefinedContextHandler(
    descriptor.methodID,
    descriptor.name,
    context) {

  override def createErrorResponse(message: String): Array[Byte] = {
    descriptor.encodeResponse(
      TransactionService.PutTransactions.Result(
        None,
        Some(ServerException(message))
      )
    )
  }

  override protected def fireAndForget(message: RequestMessage): Unit =
    tracer.withTracing(message, getClass.getName + ".fireAndForget")(process(message.body))

  override protected def getResponse(message: RequestMessage, ctx: ChannelHandlerContext): Array[Byte] = {
    tracer.withTracing(message, getClass.getName + ".getResponse") {
      val response = {
        val isPutted = process(message.body)
        if (isPutted)
          isPuttedResponse
        else
          isNotPuttedResponse
      }

      response
    }
  }

  private def process(requestBody: Array[Byte]) = {
    scheduledCommitLog.putData(
      Frame.PutTransactionsType.id.toByte,
      requestBody
    )
  }
}