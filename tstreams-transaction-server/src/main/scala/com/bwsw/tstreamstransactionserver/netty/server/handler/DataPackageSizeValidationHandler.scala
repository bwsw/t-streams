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

import com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException
import com.bwsw.tstreamstransactionserver.netty.RequestMessage
import com.bwsw.tstreamstransactionserver.netty.server.transportService.TransportValidator
import com.bwsw.tstreamstransactionserver.tracing.ServerTracer.tracer
import io.netty.channel.ChannelHandlerContext
import org.slf4j.{Logger, LoggerFactory}

class DataPackageSizeValidationHandler(nextHandler: RequestHandler,
                                       transportValidator: TransportValidator)
  extends IntermediateRequestHandler(nextHandler) {

  private val logger: Logger =
    LoggerFactory.getLogger(this.getClass)

  override def handle(message: RequestMessage,
                      ctx: ChannelHandlerContext,
                      error: Option[Throwable]): Unit = {
    tracer.withTracing(message, name = getClass.getName + ".handle") {
      if (error.isDefined) {
        if (message.isFireAndForgetMethod) {
          if (logger.isDebugEnabled())
            logger.debug(s"Client ${ctx.channel().remoteAddress().toString}", error.get)
        }
        else {
          nextHandler.handle(message, ctx, error)
        }
      }
      else {
        val isPackageTooBig = transportValidator
          .isTooBigMetadataMessage(message)

        if (isPackageTooBig)
          nextHandler.handle(
            message,
            ctx,
            Some(createError(message, ctx))
          )
        else
          nextHandler.handle(
            message,
            ctx,
            error
          )
      }
    }
  }

  private def createError(message: RequestMessage,
                          ctx: ChannelHandlerContext) = {
    new PackageTooBigException(
      s"A size of client[${ctx.channel().remoteAddress().toString}, request id: ${message.id}] request is greater " +
        s"than maxDataPackageSize (${transportValidator.maxDataPackageSize})"
    )
  }
}
