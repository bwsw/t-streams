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

import com.bwsw.tstreamstransactionserver.SinglePoolExecutionContextGrid
import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContextGrids
import com.bwsw.tstreamstransactionserver.netty.RequestMessage
import com.bwsw.tstreamstransactionserver.netty.server.authService.AuthService
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.ScheduledCommitLog
import com.bwsw.tstreamstransactionserver.netty.server.handler._
import com.bwsw.tstreamstransactionserver.netty.server.handler.auth.{AuthenticateHandler, IsValidHandler}
import com.bwsw.tstreamstransactionserver.netty.server.handler.consumer.GetConsumerStateHandler
import com.bwsw.tstreamstransactionserver.netty.server.handler.data._
import com.bwsw.tstreamstransactionserver.netty.server.handler.metadata._
import com.bwsw.tstreamstransactionserver.netty.server.handler.stream.{CheckStreamExistsHandler, DelStreamHandler, GetStreamHandler, PutStreamHandler}
import com.bwsw.tstreamstransactionserver.netty.server.handler.transport.{GetMaxPackagesSizesHandler, GetZKCheckpointGroupServerPrefixHandler}
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.commitLogService.CommitLogService
import com.bwsw.tstreamstransactionserver.netty.server.subscriber.OpenedTransactionNotifier
import com.bwsw.tstreamstransactionserver.netty.server.transportService.TransportValidator
import com.bwsw.tstreamstransactionserver.netty.server.{OrderedExecutionContextPool, TransactionServer}
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.{AuthenticationOptions, CheckpointGroupRoleOptions, TransportOptions}
import io.netty.channel.ChannelHandlerContext

import scala.collection.Searching._
import scala.concurrent.ExecutionContext
import com.bwsw.tstreamstransactionserver.netty.server.handler.RequestRouter._
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.hanlder.commitLog.GetCommitLogOffsetsHandler
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.hanlder.consumer.PutConsumerCheckpointHandler
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.hanlder.data.{PutProducerStateWithDataHandler, PutSimpleTransactionAndDataHandler, PutTransactionDataHandler}
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.hanlder.metadata.{OpenTransactionHandler, PutTransactionHandler, PutTransactionsHandler}


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

  private implicit val authService =
    new AuthService(authOptions)

  private implicit val transportValidator =
    new TransportValidator(packageTransmissionOpts)

  private val serverWriteContext: ExecutionContext =
    executionContext.serverWriteContext
  private val serverReadContext: ExecutionContext =
    executionContext.serverReadContext

  private val (handlersIDs: Array[Byte], handlers: Array[RequestHandler]) = Array(

    handlerAuth(new GetCommitLogOffsetsHandler(
      MultiNodeCommitLogService,
      scheduledCommitLog,
      serverReadContext
    )),

    handlerAuth(new PutStreamHandler(
      server,
      serverReadContext
    )),

    handlerAuth(new CheckStreamExistsHandler(
      server,
      serverReadContext
    )),

    handlerAuth(new GetStreamHandler(
      server,
      serverReadContext
    )),

    handlerAuth(new DelStreamHandler(
      server,
      serverWriteContext
    )),

    handlerAuth(new GetTransactionIDHandler(
      server
    )),
    handlerAuth(new GetTransactionIDByTimestampHandler(
      server
    )),

    handlerAuthMetadata(new PutTransactionHandler(
      server,
      scheduledCommitLog,
      commitLogContext
    )),

    handlerAuthMetadata(new PutTransactionsHandler(
      server,
      scheduledCommitLog,
      commitLogContext
    )),

    handlerAuthData(new OpenTransactionHandler(
      server,
      scheduledCommitLog,
      notifier,
      authOptions,
      orderedExecutionPool
    )),

    handlerAuth(new GetTransactionHandler(
      server,
      serverReadContext
    )),

    handlerAuth(new GetLastCheckpointedTransactionHandler(
      server,
      serverReadContext
    )),

    handlerAuth(new ScanTransactionsHandler(
      server,
      serverReadContext
    )),

    handlerAuthData(new PutProducerStateWithDataHandler(
      server,
      scheduledCommitLog,
      commitLogContext
    )),

    handlerAuthData(new PutSimpleTransactionAndDataHandler(
      server,
      scheduledCommitLog,
      notifier,
      authOptions,
      orderedExecutionPool
    )),

    handlerAuthData(new PutTransactionDataHandler(
      server,
      serverReadContext
    )),

    handlerAuth(new GetTransactionDataHandler(
      server,
      serverWriteContext
    )),

    handlerAuthMetadata(new PutConsumerCheckpointHandler(
      server,
      scheduledCommitLog,
      commitLogContext
    )),
    handlerAuth(new GetConsumerStateHandler(
      server,
      serverReadContext
    )),

    handlerId(new AuthenticateHandler(
      authService
    )),
    handlerId(new IsValidHandler(
      authService
    )),

    handlerId(new GetMaxPackagesSizesHandler(
      packageTransmissionOpts
    )),

    handlerId(new GetZKCheckpointGroupServerPrefixHandler(
      serverRoleOptions
    ))
  ).sortBy(_._1).unzip


  override def route(message: RequestMessage,
                     ctx: ChannelHandlerContext): Unit = {
    handlersIDs.search(message.methodId) match {
      case Found(index) =>
        val handler = handlers(index)
        handler.handle(message, ctx, None)
      case _ =>
//        throw new IllegalArgumentException(s"Not implemented method that has id: ${message.methodId}")
    }
  }
}
