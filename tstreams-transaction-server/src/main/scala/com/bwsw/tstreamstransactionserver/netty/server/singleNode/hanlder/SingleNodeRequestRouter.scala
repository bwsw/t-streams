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
import com.bwsw.tstreamstransactionserver.netty.server.authService.{AuthService, CommitLogTokenWriter}
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.ScheduledCommitLog
import com.bwsw.tstreamstransactionserver.netty.server.handler.RequestRouter._
import com.bwsw.tstreamstransactionserver.netty.server.handler._
import com.bwsw.tstreamstransactionserver.netty.server.handler.auth.{AuthenticateHandler, KeepAliveHandler, TokenIsValidHandler}
import com.bwsw.tstreamstransactionserver.netty.server.handler.consumer.ConsumerStateGetHandler
import com.bwsw.tstreamstransactionserver.netty.server.handler.data._
import com.bwsw.tstreamstransactionserver.netty.server.handler.metadata._
import com.bwsw.tstreamstransactionserver.netty.server.handler.stream.{StreamDeleteHandler, StreamExistsCheckHandler, StreamGetHandler, StreamPutHandler}
import com.bwsw.tstreamstransactionserver.netty.server.handler.transport.{MaxPackagesSizesGetHandler, ZKCheckpointGroupServerPrefixGetHandler}
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.commitLogService.CommitLogService
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.hanlder.commitLog.CommitLogOffsetsGetHandler
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.hanlder.consumer.ConsumerCheckpointPutHandler
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.hanlder.data.{ProducerStateWithDataPutHandler, SimpleTransactionAndDataPutHandler, TransactionDataPutHandler}
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.hanlder.metadata.{TransactionOpenHandler, TransactionPutHandler, TransactionsPutHandler}
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

  private val tokenWriter = new CommitLogTokenWriter(scheduledCommitLog)
  private implicit val authService = new AuthService(authOptions, tokenWriter)
  private implicit val transportValidator = new TransportValidator(packageTransmissionOpts)

  private val serverWriteContext: ExecutionContext = executionContext.serverWriteContext
  private val serverReadContext: ExecutionContext = executionContext.serverReadContext


  override protected val handlers: Map[Byte, RequestHandler] = Seq(
    Seq(
      new CommitLogOffsetsGetHandler(MultiNodeCommitLogService, scheduledCommitLog, serverReadContext),
      new StreamPutHandler(server, serverReadContext),
      new StreamExistsCheckHandler(server, serverReadContext),
      new StreamGetHandler(server, serverReadContext),
      new StreamDeleteHandler(server, serverWriteContext),
      new TransactionIDGetHandler(server),
      new TransactionIDByTimestampGetHandler(server),
      new TransactionGetHandler(server, serverReadContext),
      new LastCheckpointedTransactionGetHandler(server, serverReadContext),
      new TransactionsScanHandler(server, serverReadContext),
      new TransactionDataGetHandler(server, serverWriteContext),
      new ConsumerStateGetHandler(server, serverReadContext))
      .map(handlerAuth),

    Seq(
      new TransactionPutHandler(server, scheduledCommitLog, commitLogContext),
      new TransactionsPutHandler(server, scheduledCommitLog, commitLogContext),
      new ConsumerCheckpointPutHandler(server, scheduledCommitLog, commitLogContext))
      .map(handlerAuthMetadata),

    Seq(
      new TransactionOpenHandler(server, scheduledCommitLog, notifier, authOptions, orderedExecutionPool),
      new ProducerStateWithDataPutHandler(server, scheduledCommitLog, commitLogContext),
      new SimpleTransactionAndDataPutHandler(server, scheduledCommitLog, notifier, authOptions, orderedExecutionPool),
      new TransactionDataPutHandler(server, serverReadContext))
      .map(handlerAuthData),

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
