package util.netty

import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.bytes.ByteArrayEncoder

class NettyServerInitializer
  extends ChannelInitializer[SocketChannel]
{
  override def initChannel(ch: SocketChannel): Unit = {
    ch.pipeline()
      .addLast(new ByteArrayEncoder())
      .addLast(new NettyServerHandler())
  }
}
