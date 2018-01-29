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
package com.bwsw.tstreamstransactionserver.netty.server.handler.metadata

import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.handler.PredefinedContextHandler
import com.bwsw.tstreamstransactionserver.netty.server.handler.metadata.ScanTransactionsHandler.descriptor
import com.bwsw.tstreamstransactionserver.netty.{Protocol, RequestMessage}
import com.bwsw.tstreamstransactionserver.rpc.{ServerException, TransactionService}
import io.netty.channel.ChannelHandlerContext

import scala.concurrent.ExecutionContext

private object ScanTransactionsHandler {
  val descriptor = Protocol.ScanTransactions
}


class ScanTransactionsHandler(server: TransactionServer,
                              context: ExecutionContext)
  extends PredefinedContextHandler(
    descriptor.methodID,
    descriptor.name,
    context) {

  override def createErrorResponse(message: String): Array[Byte] = {
    descriptor.encodeResponse(
      TransactionService.ScanTransactions.Result(
        None,
        Some(ServerException(message)
        )
      )
    )
  }

  override protected def fireAndForget(message: RequestMessage): Unit = {}

  override protected def getResponse(message: RequestMessage, ctx: ChannelHandlerContext): Array[Byte] = {
    val response = descriptor.encodeResponse(
      TransactionService.ScanTransactions.Result(
        Some(process(message.body))
      )
    )
    response
  }

  private def process(requestBody: Array[Byte]) = {
    val args = descriptor.decodeRequest(requestBody)
    server.scanTransactions(
      args.streamID,
      args.partition,
      args.from,
      args.to,
      args.count,
      args.states
    )
  }
}
