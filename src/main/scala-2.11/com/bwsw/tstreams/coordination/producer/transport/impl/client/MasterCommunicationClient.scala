package com.bwsw.tstreams.coordination.producer.transport.impl.client

import java.io.{BufferedReader, IOException, InputStreamReader}
import java.net.{Socket, SocketTimeoutException}
import java.util.concurrent.atomic.AtomicBoolean
import com.bwsw.tstreams.common.ProtocolMessageSerializer
import com.bwsw.tstreams.common.ProtocolMessageSerializer.ProtocolMessageSerializerException
import com.bwsw.tstreams.coordination.messages.master.IMessage
import org.slf4j.LoggerFactory

import scala.collection.mutable


/**
  * Client for sending [[IMessage]]]
  */
class MasterCommunicationClient (timeoutMs: Int) {
  private val peerMap = mutable.Map[String, Socket]()
  private val serializer = new ProtocolMessageSerializer
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val isClosed = new AtomicBoolean(false)

  private def openSocket(msg: IMessage): Socket = {
    try {
      logger.debug(s"Start opening socket from peer ${msg.senderID} to peer ${msg.receiverID}.")
      val splits = msg.receiverID.split(":")
      assert(splits.size == 2)
      val host = splits(0)
      val port = splits(1).toInt
      val sock = new Socket(host, port)
      logger.debug(s"Opened socket from peer ${msg.senderID} to peer ${msg.receiverID}.")
      sock
    } catch {
      case e: IOException =>
        logger.warn(s"exception occurred: ${e.getMessage}")
        logger.warn(msg.toString())
        throw e
    }
  }

  private def getSocket(msg: IMessage): Socket = {
    var socket: Socket = null
    this.synchronized {
      if (peerMap.contains(msg.receiverID)) {
        logger.debug(s"Socket from peer ${msg.senderID} to peer ${msg.receiverID} is already known.")
        socket = peerMap(msg.receiverID)
        if (socket.isClosed || !socket.isConnected || socket.isOutputShutdown) {
          logger.debug(s"Socket from peer ${msg.senderID} to peer ${msg.receiverID} is in wrong state.")
          socket = openSocket(msg)
          socket.setSoTimeout(timeoutMs)
          peerMap(msg.receiverID) = socket
        }
      } else {
        logger.debug(s"Socket from peer ${msg.senderID} to peer ${msg.receiverID} is not known.")
        socket = openSocket(msg)
        socket.setSoTimeout(timeoutMs)
        peerMap(msg.receiverID) = socket
      }
    }
    socket
  }

  /**
    * @param msg     Message to send
    * @return Response message
    */
  def sendAndWaitResponse(msg: IMessage): IMessage = {
    if(isClosed.get)
      throw new IllegalStateException("Communication Client is closed. Unable to operate.")
    val sock = getSocket(msg)
    writeMsgAndWaitResponse(sock, msg)
  }

  /**
    * @param msg     Message to send
    * @return Response message
    */
  def sendAndNoWaitResponse(msg: IMessage):Unit = {
    if(isClosed.get)
      throw new IllegalStateException("Communication Client is closed. Unable to operate.")
    val sock = getSocket(msg)
    writeMsgAndNoWaitResponse(sock, msg)
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
  private def closeSocketAndCleanPeerMap(socket: Socket, msg: IMessage) = {
    try {
      logger.debug(s"Socket from peer ${msg.senderID} to peer ${msg.receiverID} is to be closed.")
      socket.close()
      logger.debug(s"Socket from peer ${msg.senderID} to peer ${msg.receiverID} is closed.")
    } catch {
      case e: IOException =>
        logger.warn(s"exception occurred: ${e.getMessage}")
        logger.warn(msg.toString())
    } finally {
      this.synchronized {
        peerMap.remove(msg.receiverID)
      }
    }
  }

  /**
    * Helper method for [[sendAndWaitResponse]]]
    *
    * @param sock Socket to send msg and get response
    * @param msg             Msg to send
    * @return Response message
    */
  private def writeMsgAndWaitResponse(sock: Socket, msg: IMessage): IMessage = {
    //do request
    val reqString = wrapMsg(serializer.serialize(msg))
    try {
      logger.debug(s"To send message ${reqString} from peer ${msg.senderID} to peer ${msg.receiverID}.")
      val outputStream = sock.getOutputStream
      outputStream.write(reqString.getBytes)
      outputStream.flush()
      logger.debug(s"Sent message ${reqString} from peer ${msg.senderID} to peer ${msg.receiverID}.")
    }
    catch {
      case e: IOException =>
        logger.warn(s"exception occurred: ${e.getMessage}")
        logger.warn(msg.toString())
        closeSocketAndCleanPeerMap(sock, msg)
        return null.asInstanceOf[IMessage]
    }
    //wait response with timeout
    var answer = {
      try {
        logger.debug(s"To receive response message on ${reqString} request\nsent from peer ${msg.senderID} to peer ${msg.receiverID}.")
        val reader = new BufferedReader(new InputStreamReader(sock.getInputStream))
        val string = reader.readLine()
        if (string == null)
          null.asInstanceOf[IMessage]
        else {
          val response = serializer.deserialize[IMessage](string)
          logger.debug(s"Received response message ${response} on ${reqString} request\nsent from peer ${msg.senderID} to peer ${msg.receiverID}.")
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
      closeSocketAndCleanPeerMap(sock, msg)
    }
    answer
  }

  /**
    * Helper method for [[sendAndNoWaitResponse]]]
    *
    * @param sock Socket to send msg
    * @param msg             Msg to send
    * @return Response message
    */
  private def writeMsgAndNoWaitResponse(sock: Socket, msg: IMessage) = {
    //do request
    val string = wrapMsg(serializer.serialize(msg))
    try {
      logger.debug(s"To send message ${string} from peer ${msg.senderID} to peer ${msg.receiverID}.")
      val outputStream = sock.getOutputStream
      outputStream.write(string.getBytes)
      outputStream.flush()
      logger.debug(s"Sent message ${string} from peer ${msg.senderID} to peer ${msg.receiverID}.")
    }
    catch {
      case e: IOException =>
        logger.warn(s"exception occurred: ${e.getMessage}")
        logger.warn(msg.toString())
        closeSocketAndCleanPeerMap(sock, msg)
    }
  }

  /**
    * Close client
    */
  def close() = {
    if(isClosed.getAndSet(true))
      throw new IllegalStateException("Communication Client is closed. Unable to operate.")
    peerMap.foreach { x =>
      try {
        x._2.close()
      } catch {
        case e: IOException =>
          logger.warn(s"exception occurred: ${e.getMessage}")
          throw e
      }
    }
    peerMap.clear()
  }
}

case class SocketAndReader(sock: Socket, reader: BufferedReader)