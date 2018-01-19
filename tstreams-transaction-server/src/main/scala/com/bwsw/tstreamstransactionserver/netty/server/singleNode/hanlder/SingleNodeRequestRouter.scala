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
package com.bwsw.tstreamstransactionserver.netty.server.singleNode.hanlder

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContextGrids
import com.bwsw.tstreamstransactionserver.netty.server.authService.{AuthService, TokenCommitLogWriter}
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.ScheduledCommitLog
import com.bwsw.tstreamstransactionserver.netty.server.handler.RequestRouter._
import com.bwsw.tstreamstransactionserver.netty.server.handler._
import com.bwsw.tstreamstransactionserver.netty.server.handler.auth.{AuthenticateHandler, IsValidHandler, KeepAliveHandler}
import com.bwsw.tstreamstransactionserver.netty.server.handler.consumer.GetConsumerStateHandler
import com.bwsw.tstreamstransactionserver.netty.server.handler.data._
import com.bwsw.tstreamstransactionserver.netty.server.handler.metadata._
import com.bwsw.tstreamstransactionserver.netty.server.handler.stream.{CheckStreamExistsHandler, DelStreamHandler, GetStreamHandler, PutStreamHandler}
import com.bwsw.tstreamstransactionserver.netty.server.handler.transport.{GetMaxPackagesSizesHandler, GetZKCheckpointGroupServerPrefixHandler}
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.commitLogService.CommitLogService
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.hanlder.commitLog.GetCommitLogOffsetsHandler
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.hanlder.consumer.PutConsumerCheckpointHandler
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.hanlder.data.{PutProducerStateWithDataHandler, PutSimpleTransactionAndDataHandler, PutTransactionDataHandler}
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.hanlder.metadata.{OpenTransactionHandler, PutTransactionHandler, PutTransactionsHandler}
import com.bwsw.tstreamstransactionserver.netty.server.subscriber.OpenedTransactionNotifier
import com.bwsw.tstreamstransactionserver.netty.server.transportService.TransportValidator
import com.bwsw.tstreamstransactionserver.netty.server.{OrderedExecutionContextPool, TransactionServer}
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.{AuthenticationOptions, CheckpointGroupRoleOptions, TransportOptions}

import scala.concurrent.ExecutionContext


final class SingleNodeRequestRouter(server: TransactionServer,
                                    MultiNodeCommitLogService: CommitLogService,
                                    scheduledCommitLog: ScheduledCommitLog,
                                    packageTransmissionOpts: TransportOptions,
                                    authOptions: AuthenticationOptions,
                                    orderedExecutionPool: OrderedExecutionContextPool,
                                    notifier: OpenedTransactionNotifier,
                                    serverRoleOptions: CheckpointGroupRoleOptions,
                                    executionContext: ServerExecutionContextGrids,
                                    commitLogContext: ExecutionContext)
  extends RequestRouter {

  private val tokenWriter = new TokenCommitLogWriter(scheduledCommitLog)
  private implicit val authService = new AuthService(authOptions, tokenWriter)
  private implicit val transportValidator = new TransportValidator(packageTransmissionOpts)

  private val serverWriteContext: ExecutionContext = executionContext.serverWriteContext
  private val serverReadContext: ExecutionContext = executionContext.serverReadContext


  override protected val handlers: Map[Byte, RequestHandler] = Seq(
    Seq(
      new GetCommitLogOffsetsHandler(MultiNodeCommitLogService, scheduledCommitLog, serverReadContext),
      new PutStreamHandler(server, serverReadContext),
      new CheckStreamExistsHandler(server, serverReadContext),
      new GetStreamHandler(server, serverReadContext),
      new DelStreamHandler(server, serverWriteContext),
      new GetTransactionIDHandler(server),
      new GetTransactionIDByTimestampHandler(server),
      new GetTransactionHandler(server, serverReadContext),
      new GetLastCheckpointedTransactionHandler(server, serverReadContext),
      new ScanTransactionsHandler(server, serverReadContext),
      new GetTransactionDataHandler(server, serverWriteContext),
      new GetConsumerStateHandler(server, serverReadContext))
      .map(handlerAuth),

    Seq(
      new PutTransactionHandler(server, scheduledCommitLog, commitLogContext),
      new PutTransactionsHandler(server, scheduledCommitLog, commitLogContext),
      new PutConsumerCheckpointHandler(server, scheduledCommitLog, commitLogContext))
      .map(handlerAuthMetadata),

    Seq(
      new OpenTransactionHandler(server, scheduledCommitLog, notifier, authOptions, orderedExecutionPool),
      new PutProducerStateWithDataHandler(server, scheduledCommitLog, commitLogContext),
      new PutSimpleTransactionAndDataHandler(server, scheduledCommitLog, notifier, authOptions, orderedExecutionPool),
      new PutTransactionDataHandler(server, serverReadContext))
      .map(handlerAuthData),

    Seq(
      new AuthenticateHandler(authService),
      new IsValidHandler(authService),
      new GetMaxPackagesSizesHandler(packageTransmissionOpts),
      new GetZKCheckpointGroupServerPrefixHandler(serverRoleOptions),
      new KeepAliveHandler(authService))
      .map(handlerId))
    .flatten
    .toMap
}
