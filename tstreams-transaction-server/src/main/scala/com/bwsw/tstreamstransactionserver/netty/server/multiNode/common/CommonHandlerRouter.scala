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

package com.bwsw.tstreamstransactionserver.netty.server.multiNode.common

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContextGrids
import com.bwsw.tstreamstransactionserver.netty.server.authService.{AuthService, BookKeeperTokenWriter}
import com.bwsw.tstreamstransactionserver.netty.server.handler.RequestRouter.{handlerAuth, handlerAuthData, handlerAuthMetadata, handlerId}
import com.bwsw.tstreamstransactionserver.netty.server.handler.auth.{AuthenticateHandler, KeepAliveHandler, TokenIsValidHandler}
import com.bwsw.tstreamstransactionserver.netty.server.handler.consumer.ConsumerStateGetHandler
import com.bwsw.tstreamstransactionserver.netty.server.handler.data.TransactionDataGetHandler
import com.bwsw.tstreamstransactionserver.netty.server.handler.metadata._
import com.bwsw.tstreamstransactionserver.netty.server.handler.stream.{StreamDeleteHandler, StreamExistsCheckHandler, StreamGetHandler, StreamPutHandler}
import com.bwsw.tstreamstransactionserver.netty.server.handler.transport.{MaxPackagesSizesGetHandler, ZKCheckpointGroupServerPrefixGetHandler}
import com.bwsw.tstreamstransactionserver.netty.server.handler.{RequestHandler, RequestRouter}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService.{BookkeeperMaster, BookkeeperWriter}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.commitLogService.CommitLogService
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.Utils._
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.commitLog.CommitLogOffsetsGetHandler
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.consumer.ConsumerCheckpointPutHandler
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.data.{ProducerStateWithDataPutHandler, SimpleTransactionAndDataPutHandler, TransactionDataPutHandler}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.metadata.{TransactionOpenHandler, TransactionPutHandler}
import com.bwsw.tstreamstransactionserver.netty.server.subscriber.OpenedTransactionNotifier
import com.bwsw.tstreamstransactionserver.netty.server.transportService.TransportValidator
import com.bwsw.tstreamstransactionserver.netty.server.zk.ZKMasterElector
import com.bwsw.tstreamstransactionserver.netty.server.{OrderedExecutionContextPool, TransactionServer}
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.{AuthenticationOptions, CheckpointGroupRoleOptions, TransportOptions}

import scala.concurrent.ExecutionContext

class CommonHandlerRouter(server: TransactionServer,
                          private implicit val bookkeeperWriter: BookkeeperWriter,
                          checkpointMaster: BookkeeperMaster,
                          commonMasterElector: ZKMasterElector,
                          private implicit val multiNodeCommitLogService: CommitLogService,
                          packageTransmissionOpts: TransportOptions,
                          authOptions: AuthenticationOptions,
                          orderedExecutionPool: OrderedExecutionContextPool,
                          notifier: OpenedTransactionNotifier,
                          serverRoleOptions: CheckpointGroupRoleOptions,
                          executionContext: ServerExecutionContextGrids,
                          commitLogContext: ExecutionContext)
  extends RequestRouter {

  private val tokenWriter = new BookKeeperTokenWriter(checkpointMaster, commitLogContext)
  private implicit val authService: AuthService = new AuthService(authOptions, tokenWriter)

  private implicit val transportValidator: TransportValidator = new TransportValidator(packageTransmissionOpts)

  private val serverWriteContext: ExecutionContext = executionContext.serverWriteContext
  private val serverReadContext: ExecutionContext = executionContext.serverReadContext

  private val commonMasterElectorAsSeq = Seq(commonMasterElector)


  override protected val handlers: Map[Byte, RequestHandler] = Seq(
    Seq(
      new CommitLogOffsetsGetHandler(multiNodeCommitLogService, bookkeeperWriter, serverReadContext),
      new StreamPutHandler(server, serverReadContext),
      new StreamExistsCheckHandler(server, serverReadContext),
      new StreamGetHandler(server, serverReadContext),
      new StreamDeleteHandler(server, serverWriteContext),
      new TransactionIDGetHandler(server),
      new TransactionIDByTimestampGetHandler(server))
      .map(handlerAuth),

    Seq(
      new TransactionPutHandler(checkpointMaster, commitLogContext),
      new ConsumerCheckpointPutHandler(checkpointMaster, commitLogContext))
      .map(handlerAuthMetadata),

    Seq(
      new TransactionOpenHandler(
        server, checkpointMaster, notifier, authOptions, orderedExecutionPool, commitLogContext),
      new ProducerStateWithDataPutHandler(checkpointMaster, commitLogContext),
      new SimpleTransactionAndDataPutHandler(
        server, checkpointMaster, notifier, authOptions, orderedExecutionPool, commitLogContext),
      new TransactionDataPutHandler(checkpointMaster, serverWriteContext))
      .map(handlerAuthData),

    Seq(
      new TransactionGetHandler(server, serverReadContext),
      new LastCheckpointedTransactionGetHandler(server, serverReadContext),
      new TransactionsScanHandler(server, serverReadContext),
      new ConsumerStateGetHandler(server, serverReadContext),
      new TransactionDataGetHandler(server, serverReadContext))
      .map(handlerReadAuth(_, commonMasterElectorAsSeq)),

    Seq(
      new AuthenticateHandler(authService),
      new TokenIsValidHandler(authService),
      new MaxPackagesSizesGetHandler(packageTransmissionOpts),
      new ZKCheckpointGroupServerPrefixGetHandler(serverRoleOptions),
      new KeepAliveHandler(authService))
      .map(handlerId))
    .flatten
    .toMap
}
