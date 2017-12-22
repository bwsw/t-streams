package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.netty.RequestMessage
import io.netty.channel.ChannelHandlerContext

trait RequestHandler {
  def handle(message: RequestMessage,
             ctx: ChannelHandlerContext,
             error: Option[Throwable]): Unit
}
