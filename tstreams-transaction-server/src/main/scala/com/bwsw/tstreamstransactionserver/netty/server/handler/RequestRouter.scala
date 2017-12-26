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

package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.netty.RequestMessage
import com.bwsw.tstreamstransactionserver.netty.server.authService.AuthService
import com.bwsw.tstreamstransactionserver.netty.server.transportService.TransportValidator
import io.netty.channel.ChannelHandlerContext

object RequestRouter {
  final def handlerId(clientRequestHandler: ClientRequestHandler): (Byte, RequestHandler) = {
    val id = clientRequestHandler.id
    id -> clientRequestHandler
  }

  final def handlerAuthData(clientRequestHandler: ClientRequestHandler)
                           (implicit
                            authService: AuthService,
                            transportValidator: TransportValidator): (Byte, RequestHandler) = {
    val id = clientRequestHandler.id
    id -> new AuthHandler(
      new DataPackageSizeValidationHandler(
        clientRequestHandler,
        transportValidator
      ),
      authService
    )
  }

  final def handlerAuthMetadata(clientRequestHandler: ClientRequestHandler)
                               (implicit
                                authService: AuthService,
                                transportValidator: TransportValidator): (Byte, RequestHandler) = {
    val id = clientRequestHandler.id
    id -> new AuthHandler(
      new MetadataPackageSizeValidationHandler(
        clientRequestHandler,
        transportValidator
      ),
      authService
    )
  }

  final def handlerAuth(clientRequestHandler: ClientRequestHandler)
                       (implicit
                        authService: AuthService): (Byte, RequestHandler) = {
    val id = clientRequestHandler.id
    id -> new AuthHandler(
      clientRequestHandler,
      authService
    )
  }
}

trait RequestRouter {
  def route(message: RequestMessage,
            ctx: ChannelHandlerContext): Unit
}
