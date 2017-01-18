package netty.server

import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.bytes.ByteArrayEncoder
import netty.MessageDecoder

import scala.concurrent.ExecutionContext


class ServerInitializer(server: TransactionServer, context: ExecutionContext) extends ChannelInitializer[SocketChannel] {
  override def initChannel(ch: SocketChannel): Unit = {
    ch.pipeline()
      .addLast(new ByteArrayEncoder())
      .addLast(new MessageDecoder)
      .addLast(new ServerHandler(server, context))
  }
}
