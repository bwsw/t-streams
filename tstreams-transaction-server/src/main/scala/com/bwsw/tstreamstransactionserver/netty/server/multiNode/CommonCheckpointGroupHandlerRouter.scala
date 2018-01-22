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

package com.bwsw.tstreamstransactionserver.netty.server.multiNode

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContextGrids
import com.bwsw.tstreamstransactionserver.netty.server.authService.{AuthService, BookKeeperTokenWriter}
import com.bwsw.tstreamstransactionserver.netty.server.handler.RequestRouter._
import com.bwsw.tstreamstransactionserver.netty.server.handler.auth.{AuthenticateHandler, IsValidHandler, KeepAliveHandler}
import com.bwsw.tstreamstransactionserver.netty.server.handler.consumer.GetConsumerStateHandler
import com.bwsw.tstreamstransactionserver.netty.server.handler.data.GetTransactionDataHandler
import com.bwsw.tstreamstransactionserver.netty.server.handler.metadata._
import com.bwsw.tstreamstransactionserver.netty.server.handler.stream.{CheckStreamExistsHandler, DelStreamHandler, GetStreamHandler, PutStreamHandler}
import com.bwsw.tstreamstransactionserver.netty.server.handler.transport.{GetMaxPackagesSizesHandler, GetZKCheckpointGroupServerPrefixHandler}
import com.bwsw.tstreamstransactionserver.netty.server.handler.{RequestHandler, RequestRouter}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.{BookkeeperMaster, BookkeeperWriter}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.commitLogService.CommitLogService
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.Util._
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.commitLog.GetCommitLogOffsetsHandler
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.consumer.PutConsumerCheckpointHandler
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.data.{PutProducerStateWithDataHandler, PutSimpleTransactionAndDataHandler, PutTransactionDataHandler}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.metadata.{OpenTransactionHandler, PutTransactionHandler, PutTransactionsHandler}
import com.bwsw.tstreamstransactionserver.netty.server.subscriber.OpenedTransactionNotifier
import com.bwsw.tstreamstransactionserver.netty.server.transportService.TransportValidator
import com.bwsw.tstreamstransactionserver.netty.server.zk.ZKMasterElector
import com.bwsw.tstreamstransactionserver.netty.server.{OrderedExecutionContextPool, TransactionServer}
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.{AuthenticationOptions, CheckpointGroupRoleOptions, TransportOptions}

import scala.concurrent.ExecutionContext

class CommonCheckpointGroupHandlerRouter(server: TransactionServer,
                                         private implicit val bookkeeperWriter: BookkeeperWriter,
                                         commonMaster: BookkeeperMaster,
                                         checkpointMaster: BookkeeperMaster,
                                         masterElectors: Seq[ZKMasterElector],
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


  override protected val handlers: Map[Byte, RequestHandler] = Seq(
    Seq(
      new GetCommitLogOffsetsHandler(multiNodeCommitLogService, bookkeeperWriter, serverReadContext),
      new PutStreamHandler(server, serverReadContext),
      new CheckStreamExistsHandler(server, serverReadContext),
      new GetStreamHandler(server, serverReadContext),
      new DelStreamHandler(server, serverWriteContext),
      new GetTransactionIDHandler(server),
      new GetTransactionIDByTimestampHandler(server))
      .map(handlerAuth),

    Seq(
      new PutTransactionHandler(commonMaster, commitLogContext),
      new PutTransactionsHandler(checkpointMaster, commitLogContext),
      new PutConsumerCheckpointHandler(commonMaster, commitLogContext))
      .map(handlerAuthMetadata),

    Seq(
      new OpenTransactionHandler(server, commonMaster, notifier, authOptions, orderedExecutionPool, commitLogContext),
      new PutProducerStateWithDataHandler(commonMaster, commitLogContext),
      new PutSimpleTransactionAndDataHandler(
        server, commonMaster, notifier, authOptions, orderedExecutionPool, commitLogContext),
      new PutTransactionDataHandler(commonMaster, serverWriteContext))
      .map(handlerAuthData),

    Seq(
      new GetTransactionHandler(server, serverReadContext),
      new ScanTransactionsHandler(server, serverReadContext),
      new GetTransactionDataHandler(server, serverReadContext),
      new GetConsumerStateHandler(server, serverReadContext))
      .map(handlerReadAuth(_, masterElectors)),

    Seq(
      new AuthenticateHandler(authService),
      new IsValidHandler(authService),
      new GetMaxPackagesSizesHandler(packageTransmissionOpts),
      new GetZKCheckpointGroupServerPrefixHandler(serverRoleOptions),
      new KeepAliveHandler(authService))
      .map(handlerId),

    Seq(
      new GetLastCheckpointedTransactionHandler(server, serverReadContext))
      .map(handlerReadAuthData(_, masterElectors)))
    .flatten
    .toMap
}
