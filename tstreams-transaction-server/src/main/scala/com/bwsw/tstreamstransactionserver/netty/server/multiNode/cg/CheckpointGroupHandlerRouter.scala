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

package com.bwsw.tstreamstransactionserver.netty.server.multiNode.cg

import com.bwsw.tstreamstransactionserver.netty.server.authService.{AuthService, TokenBookKeeperWriter}
import com.bwsw.tstreamstransactionserver.netty.server.handler.RequestRouter.{handlerAuthMetadata, handlerId}
import com.bwsw.tstreamstransactionserver.netty.server.handler.auth.{AuthenticateHandler, IsValidHandler, KeepAliveHandler}
import com.bwsw.tstreamstransactionserver.netty.server.handler.transport.GetMaxPackagesSizesHandler
import com.bwsw.tstreamstransactionserver.netty.server.handler.{RequestHandler, RequestRouter}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.BookkeeperMaster
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.metadata.PutTransactionsHandler
import com.bwsw.tstreamstransactionserver.netty.server.transportService.TransportValidator
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.{AuthenticationOptions, TransportOptions}

import scala.concurrent.ExecutionContext

class CheckpointGroupHandlerRouter(checkpointMaster: BookkeeperMaster,
                                   commitLogContext: ExecutionContext,
                                   packageTransmissionOpts: TransportOptions,
                                   authOptions: AuthenticationOptions)
  extends RequestRouter {

  private val tokenWriter = new TokenBookKeeperWriter(checkpointMaster, commitLogContext)
  private implicit val authService = new AuthService(authOptions, tokenWriter)
  private implicit val transportValidator = new TransportValidator(packageTransmissionOpts)

  override protected val handlers: Map[Byte, RequestHandler] = Seq(
    Seq(
      new PutTransactionsHandler(checkpointMaster, commitLogContext))
      .map(handlerAuthMetadata),

    Seq(
      new AuthenticateHandler(authService),
      new IsValidHandler(authService),
      new GetMaxPackagesSizesHandler(packageTransmissionOpts),
      new KeepAliveHandler(authService))
      .map(handlerId))
    .flatten
    .toMap
}
