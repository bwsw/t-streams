package com.bwsw.tstreams.coordination.producer.transport.impl.client

import java.io.{BufferedReader, IOException, InputStreamReader}
import java.net.{Socket, SocketTimeoutException}
import java.util.concurrent.atomic.AtomicBoolean
import com.bwsw.tstreams.common.ProtocolMessageSerializer
import com.bwsw.tstreams.common.ProtocolMessageSerializer.ProtocolMessageSerializerException
import com.bwsw.tstreams.coordination.messages.master.IMessage
import org.slf4j.LoggerFactory

import scala.collection.mutable

object CommunicationClient {
  val logger = LoggerFactory.getLogger(this.getClass)

  private def openSocket(msg: IMessage, timeoutMs: Int): Socket = {
    try {
      CommunicationClient.logger.info(s"Start opening socket from peer ${msg.senderID} to peer ${msg.receiverID}.")
      val splits = msg.receiverID.split(":")
      assert(splits.size == 2)
      val host = splits(0)
      val port = splits(1).toInt
      val sock = new Socket(host, port)
      CommunicationClient.logger.info(s"Opened socket from peer ${msg.senderID} to peer ${msg.receiverID}.")
      sock.setSoTimeout(timeoutMs)
      sock.setTcpNoDelay(true)
      sock.setKeepAlive(true)
      sock.setTrafficClass(0x10)
      sock.setPerformancePreferences(0,1,0)
      sock
    } catch {
      case e: IOException =>
        CommunicationClient.logger.warn(s"exception occurred: ${e.getMessage}")
        CommunicationClient.logger.warn(msg.toString())
        throw e
    }
  }
}

/**
  * Client for sending messages to PeerAgents
  */
class CommunicationClient(timeoutMs: Int, retryCount: Int = 3, retryDelayMs: Int = 5000) {
  private val peerMap = mutable.Map[String, Socket]()
  private val isClosed = new AtomicBoolean(false)



  private def getSocket(msg: IMessage): Socket = {
    var socket: Socket = null
    this.synchronized {
      if (peerMap.contains(msg.receiverID)) {
        if (CommunicationClient.logger.isDebugEnabled)
          CommunicationClient.logger.debug(s"Socket from peer ${msg.senderID} to peer ${msg.receiverID} is already known.")
        socket = peerMap(msg.receiverID)
        if (socket.isClosed || !socket.isConnected || socket.isOutputShutdown) {
          CommunicationClient.logger.info(s"Socket from peer ${msg.senderID} to peer ${msg.receiverID} is in wrong state.")
          socket = CommunicationClient.openSocket(msg, timeoutMs)
          peerMap(msg.receiverID) = socket
        }
      } else {
        CommunicationClient.logger.info(s"Socket from peer ${msg.senderID} to peer ${msg.receiverID} is not known.")
        socket = CommunicationClient.openSocket(msg, timeoutMs)
        peerMap(msg.receiverID) = socket
      }
    }
    socket
  }

  private def withRetryDo[TYPE](failDeterminant: TYPE, f: () => TYPE, cnt: Int, isExceptionOnFail: Boolean): TYPE = {
    if(cnt == 0) {
      if(isExceptionOnFail) throw new IllegalStateException(s"Operation is failed to complete in ${cnt} retries.")
      return failDeterminant
    }
    val rv = f()
    if(failDeterminant == rv) {
      CommunicationClient.logger.warn(s"Operation failed. Retry it for ${cnt} times more.")
      Thread.sleep(retryDelayMs)
      withRetryDo[TYPE](failDeterminant, f, cnt - 1, isExceptionOnFail)
    }
    else
      rv
  }

  /**
    * @param msg     Message to send
    * @return Response message
    */
  def sendAndWaitResponse(msg: IMessage, isExceptionOnFail: Boolean): IMessage = {
    if(isClosed.get)
      throw new IllegalStateException("Communication Client is closed. Unable to operate.")
    withRetryDo[IMessage](null, () => {
      val sock = getSocket(msg)
      val r = writeMsgAndWaitResponse(sock, msg)
      r}, retryCount, isExceptionOnFail = isExceptionOnFail)
  }



  /**
    * @param msg     Message to send
    * @return Response message
    */
  def sendAndNoWaitResponse(msg: IMessage, isExceptionOnFail: Boolean):Unit = {
    if(isClosed.get)
      throw new IllegalStateException("Communication Client is closed. Unable to operate.")
    withRetryDo[Boolean](false, () => {
      val sock = getSocket(msg)
      writeMsgAndNoWaitResponse(sock, msg)
    }, retryCount, isExceptionOnFail = isExceptionOnFail)
  }

  /**
    *
    * @param socket
    * @param msg
    */
  private def closeSocketAndCleanPeerMap(socket: Socket, msg: IMessage) = this.synchronized {
    try {
      CommunicationClient.logger.info(s"Socket from peer ${msg.senderID} to peer ${msg.receiverID} is to be closed.")
      socket.close()
      CommunicationClient.logger.info(s"Socket from peer ${msg.senderID} to peer ${msg.receiverID} is closed.")
    } catch {
      case e: IOException =>
        CommunicationClient.logger.warn(s"exception occurred: ${e.getMessage}")
        CommunicationClient.logger.warn(msg.toString())
    } finally {
      peerMap.remove(msg.receiverID)
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
    val reqString = ProtocolMessageSerializer.wrapMsg(ProtocolMessageSerializer.serialize(msg))
    try {
      if (CommunicationClient.logger.isDebugEnabled)
        CommunicationClient.logger.debug(s"To send message ${reqString} from peer ${msg.senderID} to peer ${msg.receiverID}.")
      val outputStream = sock.getOutputStream
      outputStream.write(reqString.getBytes)
      outputStream.flush()
      if (CommunicationClient.logger.isDebugEnabled)
        CommunicationClient.logger.debug(s"Sent message ${reqString} from peer ${msg.senderID} to peer ${msg.receiverID}.")
    }
    catch {
      case e: IOException =>
        CommunicationClient.logger.warn(s"exception occurred: ${e.getMessage}")
        CommunicationClient.logger.warn(msg.toString())
        closeSocketAndCleanPeerMap(sock, msg)
        return null.asInstanceOf[IMessage]
    }
    //wait response with timeout
    var answer = {
      try {
        if (CommunicationClient.logger.isDebugEnabled)
          CommunicationClient.logger.debug(s"To receive response message on ${reqString}sent from peer ${msg.senderID} to peer ${msg.receiverID}.")
        val reader = new BufferedReader(new InputStreamReader(sock.getInputStream))
        val string = reader.readLine()
        if (string == null)
          null.asInstanceOf[IMessage]
        else {
          val response = ProtocolMessageSerializer.deserialize[IMessage](string)
          if (CommunicationClient.logger.isDebugEnabled)
            CommunicationClient.logger.debug(s"Received response message ${response} on ${reqString}sent from peer ${msg.senderID} to peer ${msg.receiverID}.")
          response
        }
      }
      catch {
        case e@(_: SocketTimeoutException | _: ProtocolMessageSerializerException | _: IOException) =>
          CommunicationClient.logger.warn(s"exception occurred: ${e.getMessage}")
          CommunicationClient.logger.warn(msg.toString())
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
  private def writeMsgAndNoWaitResponse(sock: Socket, msg: IMessage): Boolean = {
    //do request
    val string = ProtocolMessageSerializer.wrapMsg(ProtocolMessageSerializer.serialize(msg))
    try {
      if (CommunicationClient.logger.isDebugEnabled)
        CommunicationClient.logger.debug(s"To send message ${string} from peer ${msg.senderID} to peer ${msg.receiverID}.")
      val outputStream = sock.getOutputStream
      outputStream.write(string.getBytes)
      outputStream.flush()
      if (CommunicationClient.logger.isDebugEnabled)
        CommunicationClient.logger.debug(s"Sent message ${string} from peer ${msg.senderID} to peer ${msg.receiverID}.")
    }
    catch {
      case e: IOException =>
        CommunicationClient.logger.warn(s"exception occurred: ${e.getMessage}")
        CommunicationClient.logger.warn(msg.toString())
        closeSocketAndCleanPeerMap(sock, msg)
        return false
    }
    return true
  }

  /**
    * Close client
    */
  def close() = this.synchronized {
    if(isClosed.getAndSet(true))
      throw new IllegalStateException("Communication Client is closed. Unable to operate.")
    peerMap.foreach { x =>
      try {
        x._2.close()
      } catch {
        case e: IOException =>
          CommunicationClient.logger.warn(s"exception occurred: ${e.getMessage}")
          throw e
      }
    }
    peerMap.clear()
  }
}

