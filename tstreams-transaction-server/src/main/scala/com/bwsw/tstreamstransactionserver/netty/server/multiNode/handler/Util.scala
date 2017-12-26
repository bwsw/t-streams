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

package com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler

import com.bwsw.tstreamstransactionserver.netty.server.authService.AuthService
import com.bwsw.tstreamstransactionserver.netty.server.handler._
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.BookkeeperWriter
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.commitLogService.CommitLogService
import com.bwsw.tstreamstransactionserver.netty.server.transportService.TransportValidator
import com.bwsw.tstreamstransactionserver.netty.server.zk.ZKMasterElector

object Util {
  final def handlerReadAuthData(clientRequestHandler: ClientRequestHandler,
                                zKMasterElectors: Seq[ZKMasterElector])
                               (implicit
                                authService: AuthService,
                                transportValidator: TransportValidator,
                                commitLogService: CommitLogService,
                                bookkeeperWriter: BookkeeperWriter): (Byte, RequestHandler) = {
    val handler =
      new ReadAllowedOperationHandler(
        new DataPackageSizeValidationHandler(
          clientRequestHandler,
          transportValidator
        ),
        commitLogService,
        bookkeeperWriter,
      )
    zKMasterElectors.foreach(_.addLeaderListener(handler))

    val id = clientRequestHandler.id
    id -> new AuthHandler(
      handler,
      authService
    )
  }

  final def handlerReadAuthMetadata(clientRequestHandler: ClientRequestHandler,
                                    zKMasterElectors: Seq[ZKMasterElector])
                                   (implicit
                                    authService: AuthService,
                                    transportValidator: TransportValidator,
                                    commitLogService: CommitLogService,
                                    bookkeeperWriter: BookkeeperWriter): (Byte, RequestHandler) = {
    val handler =
      new ReadAllowedOperationHandler(
        new MetadataPackageSizeValidationHandler(
          clientRequestHandler,
          transportValidator
        ),
        commitLogService,
        bookkeeperWriter,
      )
    zKMasterElectors.foreach(_.addLeaderListener(handler))

    val id = clientRequestHandler.id
    id -> new AuthHandler(
      handler,
      authService
    )
  }

  final def handlerReadAuth(clientRequestHandler: ClientRequestHandler,
                            zKMasterElectors: Seq[ZKMasterElector])
                           (implicit
                            authService: AuthService,
                            commitLogService: CommitLogService,
                            bookkeeperWriter: BookkeeperWriter): (Byte, RequestHandler) = {
    val handler =
      new ReadAllowedOperationHandler(
        clientRequestHandler,
        commitLogService,
        bookkeeperWriter
      )
    zKMasterElectors.foreach(_.addLeaderListener(handler))

    val id = clientRequestHandler.id
    id -> new AuthHandler(
      handler,
      authService
    )
  }
}
