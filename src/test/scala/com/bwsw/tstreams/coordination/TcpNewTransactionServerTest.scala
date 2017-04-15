package com.bwsw.tstreams.coordination

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import com.bwsw.tstreams.coordination.client.{CommunicationClient, TcpNewTransactionServer}
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 20.10.16.
  */
class TcpNewTransactionServerTest extends FlatSpec with Matchers {

  def writeMsgAndWaitResponse(sock: Socket, reqString: String): String = {
    val outputStream = sock.getOutputStream
    outputStream.write(reqString.getBytes)
    outputStream.flush()

    val reader = new BufferedReader(new InputStreamReader(sock.getInputStream))
    reader.readLine()
  }

  it should "work correctly" in {
    val transport = new TcpNewTransactionServer("0.0.0.0:8123")
    transport.start((channel, message) => {
      channel.writeAndFlush(message + "\r\n")
    })

    val socket = CommunicationClient.openSocket("127.0.0.1:8123", 1000)

    val N = 40000
    val start = System.currentTimeMillis()
    (0 until N).foreach(i => {
      writeMsgAndWaitResponse(socket, "test: $i\r\n")
    })
    val end = System.currentTimeMillis()
    println(end - start)
    println((end - start).toFloat / N.toFloat)

    transport.stopServer()

  }
}
