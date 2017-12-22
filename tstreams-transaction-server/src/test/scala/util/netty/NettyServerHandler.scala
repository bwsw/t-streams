package util.netty

import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}

class NettyServerHandler
  extends SimpleChannelInboundHandler[Array[Byte]]
{
  override def channelRead0(ctx: ChannelHandlerContext,
                            msg: Array[Byte]): Unit =
  {}
}
