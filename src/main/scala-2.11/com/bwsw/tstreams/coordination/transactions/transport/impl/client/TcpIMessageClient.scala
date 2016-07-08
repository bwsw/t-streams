package com.bwsw.tstreams.coordination.transactions.transport.impl.client

import java.io.{BufferedReader, IOException, InputStreamReader}
import java.net.{Socket, SocketTimeoutException}

import com.bwsw.tstreams.common.serializer.JsonSerializer
import com.bwsw.tstreams.coordination.transactions.messages.IMessage
import com.fasterxml.jackson.core.JsonParseException

/**
 * Client for sending [[IMessage]]]
 */
class TcpIMessageClient {
  private val addressToConnection = scala.collection.mutable.Map[String, Socket]()
  private val serializer = new JsonSerializer

  /**
   * @param msg Message to send
   * @param timeout Timeout for waiting response(null will be returned in case of timeout)
   * @return Response message
   */
  def sendAndWaitResponse(msg : IMessage, timeout : Int) : IMessage = {
    val rcvAddress = msg.receiverID
    if (addressToConnection.contains(rcvAddress)){
      val sock = addressToConnection(rcvAddress)
      if (sock.isClosed || !sock.isConnected || sock.isOutputShutdown) {
        addressToConnection.remove(rcvAddress)
        sendAndWaitResponse(msg, timeout)
      }
      else
        writeMsgAndWaitResponse(sock, msg)
    } else {
      try {
        val splits = rcvAddress.split(":")
        assert(splits.size == 2)
        val host = splits(0)
        val port = splits(1).toInt
        val sock = new Socket(host, port)
        sock.setSoTimeout(timeout*1000)
        addressToConnection(rcvAddress) = sock
        writeMsgAndWaitResponse(sock, msg)
      } catch {
        case e: IOException => null.asInstanceOf[IMessage]
      }
    }
  }

  /**
    * Wrap message with line delimiter
    * @param msg
    * @return
    */
  private def wrapMsg(msg : String): String = {
    msg + "\r\n"
  }

  /**
   * Helper method for [[sendAndWaitResponse]]]
   * @param socket Socket to send msg
   * @param msg Msg to send
   * @return Response message
   */
  private def writeMsgAndWaitResponse(socket : Socket, msg : IMessage) : IMessage = {
    //do request
    val string = wrapMsg(serializer.serialize(msg))
    try {
      val outputStream = socket.getOutputStream
      outputStream.write(string.getBytes)
      outputStream.flush()
    }
    catch {
      case e : IOException =>
        try {
          socket.close()
        } catch {
          case e : IOException =>
        } finally {
          addressToConnection.remove(msg.receiverID)
          return null.asInstanceOf[IMessage]
        }
    }
    //wait response with timeout
    val answer = {
      try {
        val reader = new BufferedReader(new InputStreamReader(socket.getInputStream))
        val string = reader.readLine()
        if (string == null)
          null.asInstanceOf[IMessage]
        else {
          val response = serializer.deserialize[IMessage](string)
          response
        }
      }
      catch {
        case e @ (_: SocketTimeoutException | _: JsonParseException | _: IOException) =>
          null.asInstanceOf[IMessage]
      }
    }
    if (answer == null) {
      socket.close()
      addressToConnection.remove(msg.receiverID)
    }

    answer
  }

  /**
   * Close client
   */
  def close() = {
    addressToConnection.foreach{ x=>
      try {
        x._2.close()
      } catch {
        case e: IOException =>
      }
    }
    addressToConnection.clear()
  }
}