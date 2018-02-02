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

import com.bwsw.tstreamstransactionserver.netty.RequestMessage
import com.bwsw.tstreamstransactionserver.netty.server.handler.{IntermediateRequestHandler, RequestHandler}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService.BookkeeperWriter
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.commitLogService.CommitLogService
import com.bwsw.tstreamstransactionserver.tracing.ServerTracer.tracer
import io.netty.channel.ChannelHandlerContext
import org.apache.curator.framework.recipes.leader.LeaderLatchListener


object ReadAllowedOperationHandler {
  val NO_LEDGER_PROCESSED: Long = -1L
}

class ReadAllowedOperationHandler(nextHandler: RequestHandler,
                                  commitLogService: CommitLogService,
                                  bookkeeperWriter: BookkeeperWriter)
  extends IntermediateRequestHandler(nextHandler)
    with LeaderLatchListener {

  @volatile private var ledgerToStartToReadFrom =
    ReadAllowedOperationHandler.NO_LEDGER_PROCESSED

  override def isLeader() = {
    ledgerToStartToReadFrom =
      bookkeeperWriter.getLastConstructedLedger
  }

  override def notLeader() = {
    ledgerToStartToReadFrom =
      ReadAllowedOperationHandler.NO_LEDGER_PROCESSED
  }


  def canPerformReadOperation: Boolean = {
    val firstLedgerId = commitLogService.getFirstLedgerId

    firstLedgerId >= ledgerToStartToReadFrom
  }

  override def handle(message: RequestMessage,
                      ctx: ChannelHandlerContext,
                      error: Option[Throwable]): Unit = {
    tracer.withTracing(message, name = getClass.getName + ".handle") {
      if (canPerformReadOperation)
        nextHandler.handle(message, ctx, error)
    }
  }
}
