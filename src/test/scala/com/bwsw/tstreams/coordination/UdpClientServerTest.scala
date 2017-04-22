package com.bwsw.tstreams.coordination

import java.net.{DatagramPacket, SocketAddress}
import java.util.concurrent.CountDownLatch

import com.bwsw.tstreams.common.UdpClient
import com.bwsw.tstreams.coordination.server.RequestsServer
import com.bwsw.tstreams.generator.LocalTransactionGenerator
import com.bwsw.tstreams.proto.protocol.{TransactionRequest, TransactionResponse}
import org.scalatest.{FlatSpec, Matchers}
import org.slf4j.LoggerFactory

/**
  * Created by ivan on 19.04.17.
  */
class UdpClientServerTest extends FlatSpec with Matchers {

  val logger = LoggerFactory.getLogger(this.getClass)

  it should "operate" in {

    val g = new LocalTransactionGenerator()

    val server = new RequestsServer("127.0.0.1", 8123, 8) {
      override def handleRequest(client: SocketAddress, reqAny: AnyRef): Unit = {
        val req = reqAny.asInstanceOf[TransactionRequest]
        val response = TransactionResponse()
          .withId(req.id)
          .withTransaction(g.getTransaction())
          .toByteArray
        socket.send(new DatagramPacket(response, response.size, client))
      }
    }.start()

    val client = new UdpClient(1000).start()

    val N = 100000
    val NT = 1
    val lBegin = new CountDownLatch(NT)
    val lEnd = new CountDownLatch(NT)

    val threads = (0 until NT).map(_ => new Thread(() => {
      lBegin.countDown()
      lBegin.await()
      (0 until N).foreach(i => {
        val data = new Array[Byte](400)
        val resOpt = client.sendAndWait("127.0.0.1", 8123, TransactionRequest(partition = i % 8,
          isInstant = true, data = Seq(com.google.protobuf.ByteString.copyFrom(data))))
        resOpt.isDefined shouldBe true
      })
      lEnd.countDown()
      lEnd.await()
    }))

    threads.foreach(t => t.start())

    lBegin.await()
    val stime = System.currentTimeMillis()

    lEnd.await()
    val etime = System.currentTimeMillis()

    logger.info(s"Request time: ${(etime - stime) * 1.0f / N / NT}")
    logger.info(s"Requests per second on client side: ${1000f / ((etime - stime) * 1.0f / N / NT)}")
    logger.info(s"Requests per ms: ${N * NT * 1.0f / (etime - stime)}")

    client.stop()
    server.stop()

  }
}





