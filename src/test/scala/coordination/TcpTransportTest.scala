package coordination

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import com.bwsw.tstreams.coordination.client.{CommunicationClient, TcpTransport}
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 20.10.16.
  */
class TcpTransportTest extends FlatSpec with Matchers {

  def writeMsgAndWaitResponse(sock: Socket, reqString: String): String = {
    val outputStream = sock.getOutputStream
    outputStream.write(reqString.getBytes)
    outputStream.flush()

    val reader = new BufferedReader(new InputStreamReader(sock.getInputStream))
    reader.readLine()
  }

  it should "work correctly" in {
    val transport = new TcpTransport("0.0.0.0:8123", 1000)
    transport.start((channel, message) => {
      channel.writeAndFlush(message + "\r\n")
    })

    val socket = CommunicationClient.openSocket("127.0.0.1:8123", 1000)

    val N = 4000
    val start = System.currentTimeMillis()
    (0 until N).foreach(i => {
      writeMsgAndWaitResponse(socket, "test: $i\r\n")
    })
    val end = System.currentTimeMillis()
    println(end - start)
    println((end - start).toFloat / N.toFloat)

    transport.stopServer()
    transport.stopClient()

  }
}
