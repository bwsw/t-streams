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

import com.bwsw.tstreamstransactionserver.netty.{RequestMessage, Protocol}
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.rpc.{ServerException, TransactionService}
import GetTransactionIDProcessor.descriptor
import com.bwsw.tstreamstransactionserver.netty.server.handler.SyncReadClientRequestHandler
import io.netty.channel.ChannelHandlerContext


private object GetTransactionIDProcessor {
  val descriptor = Protocol.GetTransactionID
}

class GetTransactionIDProcessor(server: TransactionServer)
  extends SyncReadClientRequestHandler(
    descriptor.methodID,
    descriptor.name
  ){

  private def process(requestBody: Array[Byte]) = {
    server.getTransactionID
  }


  override protected def fireAndReplyImplementation(message: RequestMessage,
                                                    ctx: ChannelHandlerContext,
                                                    acc: Option[Throwable]): Array[Byte] = {
    if (acc.isEmpty) {
      val response = descriptor.encodeResponse(
        TransactionService.GetTransactionID.Result(
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

  override def createErrorResponse(message: String): Array[Byte] = {
    descriptor.encodeResponse(
      TransactionService.GetTransactionID.Result(
        None,
        Some(ServerException(message)
        )
      )
    )
  }
}
