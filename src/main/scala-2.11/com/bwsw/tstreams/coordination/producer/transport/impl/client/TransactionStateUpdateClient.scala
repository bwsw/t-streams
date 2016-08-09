package com.bwsw.tstreams.coordination.producer.transport.impl.client

import java.io.{BufferedReader, IOException, InputStreamReader}
import java.net.{Socket, SocketTimeoutException}
import com.bwsw.tstreams.common.ProtocolMessageSerializer
import com.bwsw.tstreams.common.ProtocolMessageSerializer.ProtocolMessageSerializerException
import com.bwsw.tstreams.coordination.messages.master.IMessage
import org.slf4j.LoggerFactory

import scala.collection.mutable


/**
  * Client for sending [[IMessage]]]
  */
class TransactionStateUpdateClient {
  private val addressToConnection = mutable.Map[String, Socket]()
  private val serializer = new ProtocolMessageSerializer
  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * @param msg     Message to send
    * @param timeout Timeout for waiting response(null will be returned in case of timeout)
    * @return Response message
    */
  def sendAndWaitResponse(msg: IMessage, timeout: Int): IMessage = {
    val rcvAddress = msg.receiverID
    if (addressToConnection.contains(rcvAddress)) {
      val sock = addressToConnection(rcvAddress)
      val reader = new BufferedReader(new InputStreamReader(sock.getInputStream))
      val socketAndReader = SocketAndReader(sock, reader)
      if (sock.isClosed || !sock.isConnected || sock.isOutputShutdown) {
        addressToConnection.remove(rcvAddress)
        sendAndWaitResponse(msg, timeout)
      }
      else
        writeMsgAndWaitResponse(socketAndReader, msg)
    } else {
      try {
        val splits = rcvAddress.split(":")
        assert(splits.size == 2)
        val host = splits(0)
        val port = splits(1).toInt
        val sock = new Socket(host, port)
        val reader = new BufferedReader(new InputStreamReader(sock.getInputStream))
        val socketAndReader = SocketAndReader(sock, reader)
        sock.setSoTimeout(timeout * 1000)
        addressToConnection(rcvAddress) = sock
        writeMsgAndWaitResponse(socketAndReader, msg)
      } catch {
        case e: IOException =>
          logger.warn(s"exception occurred: ${e.getMessage}")
          logger.warn(msg.toString())
          null.asInstanceOf[IMessage]
      }
    }
  }

  /**
    * @param msg     Message to send
    * @return Response message
    */
  def sendAndNoWaitResponse(msg: IMessage):Unit = {
    val rcvAddress = msg.receiverID
    if (addressToConnection.contains(rcvAddress)) {
      val sock = addressToConnection(rcvAddress)
      val reader = new BufferedReader(new InputStreamReader(sock.getInputStream))
      val socketAndReader = SocketAndReader(sock, reader)
      if (sock.isClosed || !sock.isConnected || sock.isOutputShutdown) {
        addressToConnection.remove(rcvAddress)
        sendAndNoWaitResponse(msg)
      }
      else
        writeMsgAndNoWaitResponse(socketAndReader, msg)
    } else {
      try {
        val splits = rcvAddress.split(":")
        assert(splits.size == 2)
        val host = splits(0)
        val port = splits(1).toInt
        val sock = new Socket(host, port)
        val socketAndReader = SocketAndReader(sock, null)
        addressToConnection(rcvAddress) = sock
        writeMsgAndNoWaitResponse(socketAndReader, msg)
      } catch {
        case e: IOException =>
          logger.warn(s"exception occurred: ${e.getMessage}")
          logger.warn(msg.toString())
          null.asInstanceOf[IMessage]
      }
    }
  }

  /**
    * Wrap message with line delimiter to separate it on server side
    *
    * @param msg
    * @return
    */
  private def wrapMsg(msg: String): String = {
    msg + "\n"
  }

  /**
    *
    * @param socket
    * @param msg
    */
  private def closeSocketAndUpdateMap(socket: Socket, msg: IMessage) = {
    try {
      socket.close()
    } catch {
      case e: IOException =>
        logger.warn(s"exception occurred: ${e.getMessage}")
        logger.warn(msg.toString())
    } finally {
      addressToConnection.remove(msg.receiverID)
    }
  }

  /**
    * Helper method for [[sendAndWaitResponse]]]
    *
    * @param socketAndReader Socket and its reader to send msg
    * @param msg             Msg to send
    * @return Response message
    */
  private def writeMsgAndWaitResponse(socketAndReader: SocketAndReader, msg: IMessage): IMessage = {
    //do request
    val string = wrapMsg(serializer.serialize(msg))
    try {
      val outputStream = socketAndReader.sock.getOutputStream
      outputStream.write(string.getBytes)
      outputStream.flush()
    }
    catch {
      case e: IOException =>
        logger.warn(s"exception occurred: ${e.getMessage}")
        logger.warn(msg.toString())
        closeSocketAndUpdateMap(socketAndReader.sock, msg)
        return null.asInstanceOf[IMessage]
    }
    //wait response with timeout
    var answer = {
      try {
        val string = socketAndReader.reader.readLine()
        if (string == null)
          null.asInstanceOf[IMessage]
        else {
          val response = serializer.deserialize[IMessage](string)
          response
        }
      }
      catch {
        case e@(_: SocketTimeoutException | _: ProtocolMessageSerializerException | _: IOException) =>
          logger.warn(s"exception occurred: ${e.getMessage}")
          logger.warn(msg.toString())
          null.asInstanceOf[IMessage]
      }
    }
    if (answer == null || msg.msgID != answer.msgID) {
      answer = null
      closeSocketAndUpdateMap(socketAndReader.sock, msg)
    }

    answer
  }

  /**
    * Helper method for [[sendAndNoWaitResponse]]]
    *
    * @param socketAndReader Socket and its reader to send msg
    * @param msg             Msg to send
    * @return Response message
    */
  private def writeMsgAndNoWaitResponse(socketAndReader: SocketAndReader, msg: IMessage) = {
    //do request
    val string = wrapMsg(serializer.serialize(msg))
    try {
      val outputStream = socketAndReader.sock.getOutputStream
      outputStream.write(string.getBytes)
      outputStream.flush()
    }
    catch {
      case e: IOException =>
        logger.warn(s"exception occurred: ${e.getMessage}")
        logger.warn(msg.toString())
        closeSocketAndUpdateMap(socketAndReader.sock, msg)
    }
  }

  /**
    * Close client
    */
  def close() = {
    addressToConnection.foreach { x =>
      try {
        x._2.close()
      } catch {
        case e: IOException =>
          logger.warn(s"exception occurred: ${e.getMessage}")
          throw e
      }
    }
    addressToConnection.clear()
  }
}

case class SocketAndReader(sock: Socket, reader: BufferedReader)