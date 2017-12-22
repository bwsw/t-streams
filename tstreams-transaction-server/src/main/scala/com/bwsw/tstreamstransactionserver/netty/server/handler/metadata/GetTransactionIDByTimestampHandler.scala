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
import com.bwsw.tstreamstransactionserver.netty.server.handler.SyncReadHandler
import com.bwsw.tstreamstransactionserver.netty.server.handler.metadata.GetTransactionIDByTimestampHandler.descriptor
import com.bwsw.tstreamstransactionserver.netty.{Protocol, RequestMessage}
import com.bwsw.tstreamstransactionserver.rpc.{ServerException, TransactionService}
import io.netty.channel.ChannelHandlerContext


private object GetTransactionIDByTimestampHandler {
  val descriptor = Protocol.GetTransactionIDByTimestamp
}

class GetTransactionIDByTimestampHandler(server: TransactionServer)
  extends SyncReadHandler(
    descriptor.methodID,
    descriptor.name
  ) {

  override protected def getResponse(message: RequestMessage,
                                     ctx: ChannelHandlerContext,
                                     acc: Option[Throwable]): Array[Byte] = {
    if (acc.isEmpty) {
      val response = descriptor.encodeResponse(
        TransactionService.GetTransactionIDByTimestamp.Result(
          Some(process(message.body))
        )
      )
      response
    } else {
      val error = acc.get
      logUnsuccessfulProcessing(name, error, message, ctx)
      createErrorResponse(error.getMessage)
    }
  }

  private def process(requestBody: Array[Byte]) = {
    val args = descriptor.decodeRequest(requestBody)
    server.getTransactionIDByTimestamp(args.timestamp)
  }

  override def createErrorResponse(message: String): Array[Byte] = {
    descriptor.encodeResponse(
      TransactionService.GetTransactionIDByTimestamp.Result(
        None,
        Some(ServerException(message)
        )
      )
    )
  }
}
