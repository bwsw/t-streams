package com.bwsw.tstreams.coordination

import java.net.{DatagramSocket, InetAddress}
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.coordination.server.UdpMessageServer
import com.bwsw.tstreams.proto.protocol.TransactionState
import io.netty.channel.socket.DatagramPacket
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import io.netty.util.CharsetUtil
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by ivan on 11.04.17.
  */
class UdpMessageServerTest extends FlatSpec with Matchers {
  it should "deliver messages correctly" in {
    val l = new CountDownLatch(1)
    var m = 0L
    val srv = new UdpMessageServer("127.0.0.1", 8123, new SimpleChannelInboundHandler[DatagramPacket] {
      override def channelRead0(ctx: ChannelHandlerContext, msg: DatagramPacket): Unit = {
        try {
          val bytes = new Array[Byte](msg.content().readableBytes())
          val buf = msg.content().readBytes(bytes)
          val proto = TransactionState.parseFrom(bytes)
          m = proto.transactionID
        } catch {
          case e: Exception => println(e)
        } finally {
          l.countDown()
        }
      }
    })
    srv.start()
    val clientSocket = new DatagramSocket()
    val host = InetAddress.getByName("127.0.0.1")
    val port = 8123
    val bytes = TransactionState(transactionID = 13L).toByteArray
    println(bytes)
    val sendPacket = new java.net.DatagramPacket(bytes, bytes.length, host, port)
    clientSocket.send(sendPacket)
    l.await(2, TimeUnit.SECONDS)
    srv.stop()
    m shouldBe 13L
  }
}
