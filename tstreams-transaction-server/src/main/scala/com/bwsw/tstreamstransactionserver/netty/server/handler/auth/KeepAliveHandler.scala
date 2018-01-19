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

package com.bwsw.tstreamstransactionserver.netty.server.handler.auth

import com.bwsw.tstreamstransactionserver.netty.server.authService.AuthService
import com.bwsw.tstreamstransactionserver.netty.server.handler.SyncReadHandler
import com.bwsw.tstreamstransactionserver.netty.{Protocol, RequestMessage}
import com.bwsw.tstreamstransactionserver.rpc.TransactionService
import io.netty.channel.ChannelHandlerContext

import scala.util.{Failure, Success, Try}

/** Handler for keep-alive request
  *
  * @author Pavel Tomskikh
  */
class KeepAliveHandler(authService: AuthService)
  extends SyncReadHandler(
    KeepAliveHandler.descriptor.methodID,
    KeepAliveHandler.descriptor.name) {

  import KeepAliveHandler.descriptor

  override protected def getResponse(message: RequestMessage,
                                     ctx: ChannelHandlerContext,
                                     error: Option[Throwable]): Array[Byte] =
    createResponse(authService.update(message.token))

  override def createErrorResponse(message: String): Array[Byte] =
    createResponse(false)

  private def createResponse(result: Boolean) = {
    descriptor.encodeResponse(
      TransactionService.KeepAlive.Result(Some(result)))
  }
}


private object KeepAliveHandler {
  private val descriptor = Protocol.KeepAlive
}
