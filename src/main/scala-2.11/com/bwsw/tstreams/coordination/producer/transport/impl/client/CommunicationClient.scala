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

  private def openSocket(address: String, timeoutMs: Int): Socket = {
    try {
      CommunicationClient.logger.info(s"Start opening socket to peer ${address}.")
      val splits = address.split(":")
      assert(splits.size == 2)
      val host = splits(0)
      val port = splits(1).toInt
      val sock = new Socket(host, port)
      CommunicationClient.logger.info(s"Opened socket to peer ${address}.")
      sock.setSoTimeout(timeoutMs)
      sock.setTcpNoDelay(true)
      sock.setKeepAlive(true)
      sock.setTrafficClass(0x10)
      sock.setPerformancePreferences(0,1,0)
      sock
    } catch {
      case e: IOException =>
        CommunicationClient.logger.warn(s"exception occurred when opening connection to peer ${address}: ${e.getMessage}")
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



  private def getSocket(address: String): Socket = this.synchronized {
    var socket: Socket = null
    this.synchronized {
      if (peerMap.contains(address)) {
        if (CommunicationClient.logger.isDebugEnabled)
          CommunicationClient.logger.debug(s"Socket to peer ${address} is already known.")
        socket = peerMap(address)
        if (socket.isClosed || !socket.isConnected || socket.isOutputShutdown) {
          CommunicationClient.logger.info(s"Socket to peer ${address} is is in wrong state.")
          socket = CommunicationClient.openSocket(address, timeoutMs)
          peerMap(address) = socket
        }
      } else {
        CommunicationClient.logger.info(s"Socket fto peer ${address} is not known.")
        socket = CommunicationClient.openSocket(address, timeoutMs)
        peerMap(address) = socket
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
      val sock = getSocket(msg.receiverID)
      val reqString = ProtocolMessageSerializer.wrapMsg(ProtocolMessageSerializer.serialize(msg))
      val r = writeMsgAndWaitResponse(sock, reqString)
      if(msg.msgID != r.msgID)
        throw new IllegalStateException(s"Sent message with ID ${msg.msgID}, received ${r.msgID}. ID must be the same.")
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
      val sock = getSocket(msg.receiverID)
      val reqString = ProtocolMessageSerializer.wrapMsg(ProtocolMessageSerializer.serialize(msg))
      writeMsgAndNoWaitResponse(sock, reqString)
    }, retryCount, isExceptionOnFail = isExceptionOnFail)
  }

  /**
    *
    * @param socket
    */
  private def closeSocketAndCleanPeerMap(socket: Socket) = this.synchronized {
    val addr = socket.getInetAddress.toString
    try {
      CommunicationClient.logger.info(s"Socket to peer ${socket.getInetAddress.toString} is to be closed.")
      socket.close()
      CommunicationClient.logger.info(s"Socket to peer ${socket.getInetAddress.toString} is closed.")
    } catch {
      case e: IOException =>
        CommunicationClient.logger.warn(s"exception occurred: ${e.getMessage}")
    } finally {
      peerMap.remove(addr)
    }
  }

  /**
    * Helper method for [[sendAndWaitResponse]]]
    *
    * @param sock Socket to send msg and get response
    * @param reqString            Msg to send
    * @return Response message
    */
  private def writeMsgAndWaitResponse(sock: Socket, reqString: String): IMessage = {
    try {
      if (CommunicationClient.logger.isDebugEnabled)
        CommunicationClient.logger.debug(s"To send message ${reqString} to peer ${sock.getInetAddress.toString}.")
      val outputStream = sock.getOutputStream
      outputStream.write(reqString.getBytes)
      outputStream.flush()
      if (CommunicationClient.logger.isDebugEnabled)
        CommunicationClient.logger.debug(s"Sent message ${reqString} to peer ${sock.getInetAddress.toString}.")
    }
    catch {
      case e: IOException =>
        CommunicationClient.logger.warn(s"exception occurred when sending request to peer ${sock.getInetAddress.toString}: ${e.getMessage}")
        return null.asInstanceOf[IMessage]
    }
    //wait response with timeout
    var answer = {
      try {
        if (CommunicationClient.logger.isDebugEnabled)
          CommunicationClient.logger.debug(s"To receive response message on ${reqString} sent to peer ${sock.getInetAddress.toString}.")
        val reader = new BufferedReader(new InputStreamReader(sock.getInputStream))
        val string = reader.readLine()
        if (string == null)
          null.asInstanceOf[IMessage]
        else {
          val response = ProtocolMessageSerializer.deserialize[IMessage](string)
          if (CommunicationClient.logger.isDebugEnabled)
            CommunicationClient.logger.debug(s"Received response message ${response} on ${reqString} sent to peer ${sock.getInetAddress.toString}.")
          response
        }
      }
      catch {
        case e@(_: SocketTimeoutException | _: ProtocolMessageSerializerException | _: IOException) =>
          CommunicationClient.logger.warn(s"exception occurred when receiving response from peer ${sock.getInetAddress.toString}: ${e.getMessage}")
          null.asInstanceOf[IMessage]
      }
    }
    if (answer == null) {
      answer = null
      closeSocketAndCleanPeerMap(sock)
    }
    answer
  }

  /**
    * Helper method for [[sendAndNoWaitResponse]]]
    *
    * @param sock Socket to send msg
    * @param reqString  Msg to send
    * @return true or false if operation failed
    */
  private def writeMsgAndNoWaitResponse(sock: Socket, reqString: String): Boolean = {
    //do request
    try {
      if (CommunicationClient.logger.isDebugEnabled)
        CommunicationClient.logger.debug(s"To send message ${reqString} to peer ${sock.getInetAddress.toString}.")
      val outputStream = sock.getOutputStream
      outputStream.write(reqString.getBytes)
      outputStream.flush()
      if (CommunicationClient.logger.isDebugEnabled)
        CommunicationClient.logger.debug(s"Sent message ${reqString} to peer ${sock.getInetAddress.toString}.")
    }
    catch {
      case e: IOException =>
        CommunicationClient.logger.warn(s"exception occurred when receiving response from peer ${sock.getInetAddress.toString}: ${e.getMessage}")
        closeSocketAndCleanPeerMap(sock)
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

