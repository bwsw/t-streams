package com.bwsw.tstreams.coordination.client

import java.io.{BufferedReader, IOException, InputStreamReader}
import java.net.{ConnectException, Socket, SocketTimeoutException}
import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreams.common.ProtocolMessageSerializer
import com.bwsw.tstreams.common.ProtocolMessageSerializer.ProtocolMessageSerializerException
import com.bwsw.tstreams.coordination.messages.master.IMessage
import com.bwsw.tstreams.coordination.messages.state.TransactionStateMessage
import org.slf4j.LoggerFactory

import scala.collection.mutable

object CommunicationClient {
  val logger = LoggerFactory.getLogger(this.getClass)

  private def openSocket(address: String, timeoutMs: Int): Socket = {
    try {
      CommunicationClient.logger.info(s"Start opening socket to peer $address.")
      val splits = address.split(":")
      assert(splits.size == 2)
      val host = splits(0)
      val port = splits(1).toInt
      val sock = new Socket(host, port)
      CommunicationClient.logger.info(s"Opened socket to peer $address.")
      sock.setSoTimeout(timeoutMs)
      sock.setTcpNoDelay(true)
      sock.setKeepAlive(true)
      sock.setTrafficClass(0x10)
      sock.setPerformancePreferences(0, 1, 0)
      sock
    } catch {
      case e@(_: ConnectException | _: IOException) =>
        CommunicationClient.logger.warn(s"An exception occurred when opening connection to peer $address: ${e.getMessage}")
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
          CommunicationClient.logger.debug(s"Socket to peer $address is already known.")
        socket = peerMap(address)
        if (socket.isClosed || !socket.isConnected || socket.isOutputShutdown) {
          CommunicationClient.logger.info(s"Socket to peer $address is is in wrong state.")
          socket = CommunicationClient.openSocket(address, timeoutMs)
          peerMap(address) = socket
        }
      } else {
        CommunicationClient.logger.info(s"Socket to peer $address is not known.")
        socket = CommunicationClient.openSocket(address, timeoutMs)
        peerMap(address) = socket
      }
    }
    socket
  }

  private def withRetryDo[TYPE](failDeterminant: TYPE, f: () => TYPE, count: Int, isExceptionOnFail: Boolean): TYPE = {
    if (count == 0) {
      if (isExceptionOnFail) throw new IllegalStateException(s"Operation is failed to complete in $count retries.")
      return failDeterminant
    }
    val rv = f()
    if (failDeterminant == rv) {
      CommunicationClient.logger.warn(s"Operation failed. Retry it for $count times more.")
      Thread.sleep(retryDelayMs)
      withRetryDo[TYPE](failDeterminant, f, count - 1, isExceptionOnFail)
    }
    else
      rv
  }

  /**
    * @param msg Message to send
    * @return Response message
    */
  def sendAndWaitResponse(msg: IMessage, isExceptionIfFails: Boolean, onFailCallback: () => IMessage): IMessage = {
    if (isClosed.get)
      throw new IllegalStateException("Communication Client is closed. Unable to operate.")

    withRetryDo[IMessage](null, () => {
      try {
        val sock = getSocket(msg.receiverID)
        val reqString = ProtocolMessageSerializer.wrapMsg(ProtocolMessageSerializer.serialize(msg))
        val r = writeMsgAndWaitResponse(sock, reqString)
        if (r != null && msg.msgID != r.msgID) {
          CommunicationClient.logger.error("Request: " + msg.toString())
          CommunicationClient.logger.error("Response: " + r.toString())
          throw new IllegalStateException(s"Sent message with ID ${msg.msgID}, received ${r.msgID}. ID must be the same.")

        }
        r
      } catch {
        case e@(_: ConnectException | _: IOException) =>
          onFailCallback()
          null
      }
    }, retryCount, isExceptionOnFail = isExceptionIfFails)
  }

  /**
    * @param msg Message to send
    * @return Response message
    */
  def sendAndWaitResponseSimple(msg: IMessage): IMessage = {
    if (isClosed.get)
      throw new IllegalStateException("Communication Client is closed. Unable to operate.")

    CommunicationClient.logger.info(msg.toString())

    try {
      val sock = getSocket(msg.receiverID)
      val reqString = ProtocolMessageSerializer.wrapMsg(ProtocolMessageSerializer.serialize(msg))
      val r = writeMsgAndWaitResponse(sock, reqString)
      if (r != null && msg.msgID != r.msgID)
        throw new IllegalStateException(s"Sent message with ID ${msg.msgID}, received ${r.msgID}. ID must be the same.")
      r
    } catch {
      case e@(_: ConnectException | _: IOException) =>
        null
    }
  }

  /**
    * @param msg Message to send
    * @return Response message
    */
  def sendAndNoWaitResponse(msg: IMessage, isExceptionIfFails: Boolean, onFailCallback: () => Boolean): Boolean = {
    if (isClosed.get)
      throw new IllegalStateException("Communication Client is closed. Unable to operate.")
    withRetryDo[Boolean](false, () => {
      try {
        val sock = getSocket(msg.receiverID)
        val reqString = ProtocolMessageSerializer.wrapMsg(ProtocolMessageSerializer.serialize(msg))
        writeMsgAndNoWaitResponse(sock, reqString)
      } catch {
        case e@(_: ConnectException | _: IOException) =>
          onFailCallback()
      }
    }, retryCount, isExceptionOnFail = isExceptionIfFails)
  }

  /**
    *
    * @param peers
    */
  def initConnections(peers: Set[String]) = {
    peers.foreach(peer =>
      try {
        peerMap get peer foreach (s => closeSocketAndCleanPeerMap(s))
        val s = getSocket(peer)
      } catch {
        case e@(_: ConnectException | _: IOException) =>
          CommunicationClient.logger.warn(s"An exception occurred when opening connection to peer $peer: ${e.getMessage}")
      })
  }

  /**
    * Send broadcast message to several recipients
    *
    * @param peers
    * @param msg
    * @return Set of not  peers (to exclude failed from further send-outs until next update)
    */
  def broadcast(peers: Set[String], msg: TransactionStateMessage): Set[String] = {
    val req = ProtocolMessageSerializer
      .wrapMsg(ProtocolMessageSerializer
        .serialize(msg))

    peers.filter(peer =>
      try {
        val r = writeMsgAndNoWaitResponse(getSocket(peer), req)
        r
      } catch {
        case e@(_: ConnectException | _: IOException) =>
          CommunicationClient.logger.warn(s"An exception occurred when opening connection to peer $peer: ${e.getMessage}")
          false // failed
      })
  }

  /**
    *
    * @param socket
    */
  private def closeSocketAndCleanPeerMap(socket: Socket) = this.synchronized {
    val address = socket.getInetAddress.toString
    try {
      CommunicationClient.logger.info(s"Socket to peer ${socket.getInetAddress.toString}:${socket.getPort} is to be closed.")
      socket.close()
      CommunicationClient.logger.info(s"Socket to peer ${socket.getInetAddress.toString}:${socket.getPort} is closed.")
    } catch {
      case e@(_: ConnectException | _: IOException) =>
        CommunicationClient.logger.warn(s"An exception occurred: ${e.getMessage}")
    } finally {
      peerMap.remove(address)
    }
  }

  /**
    * Helper method for [[sendAndWaitResponse]]]
    *
    * @param sock      Socket to send msg and get response
    * @param reqString Msg to send
    * @return Response message
    */
  private def writeMsgAndWaitResponse(sock: Socket, reqString: String): IMessage = {
    try {
      if (CommunicationClient.logger.isDebugEnabled)
        CommunicationClient.logger.debug(s"To send message $reqString to peer ${sock.getInetAddress.toString}:${sock.getPort}.")
      val outputStream = sock.getOutputStream
      outputStream.write(reqString.getBytes)
      outputStream.flush()
      if (CommunicationClient.logger.isDebugEnabled)
        CommunicationClient.logger.debug(s"Sent message $reqString to peer ${sock.getInetAddress.toString}:${sock.getPort}.")
    }
    catch {
      case e@(_: ConnectException | _: IOException) =>
        CommunicationClient.logger.warn(s"An exception occurred when sending request to peer ${sock.getInetAddress.toString}: ${e.getMessage}")
        return null.asInstanceOf[IMessage]
    }
    //wait response with timeout
    var answer = {
      try {
        if (CommunicationClient.logger.isDebugEnabled)
          CommunicationClient.logger.debug(s"To receive response message on $reqString sent to peer ${sock.getInetAddress.toString}:${sock.getPort}.")
        val reader = new BufferedReader(new InputStreamReader(sock.getInputStream))
        val string = reader.readLine()
        if (string == null)
          null.asInstanceOf[IMessage]
        else {
          val response = ProtocolMessageSerializer.deserialize[IMessage](string)
          if (CommunicationClient.logger.isDebugEnabled)
            CommunicationClient.logger.debug(s"Received response message $response on $reqString sent to peer ${sock.getInetAddress.toString}:${sock.getPort}.")
          response
        }
      }
      catch {
        case e@(_: SocketTimeoutException | _: ProtocolMessageSerializerException | _: IOException | _: ConnectException) =>
          CommunicationClient.logger.warn(s"An exception occurred when receiving response from peer ${sock.getInetAddress.toString}:${sock.getPort} -  ${e.getMessage}")
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
    * @param sock      Socket to send msg
    * @param reqString Msg to send
    * @return true or false if operation failed
    */
  private def writeMsgAndNoWaitResponse(sock: Socket, reqString: String): Boolean = {
    //do request
    try {
      if (CommunicationClient.logger.isDebugEnabled)
        CommunicationClient.logger.debug(s"To send message $reqString to peer ${sock.getInetAddress.toString}:${sock.getPort}.")
      val outputStream = sock.getOutputStream
      outputStream.write(reqString.getBytes)
      outputStream.flush()
      if (CommunicationClient.logger.isDebugEnabled)
        CommunicationClient.logger.debug(s"Sent message $reqString to peer ${sock.getInetAddress.toString}:${sock.getPort}.")
    }
    catch {
      case e@(_: ConnectException | _: IOException) =>
        CommunicationClient.logger.warn(s"An exception occurred when sending response to peer ${sock.getInetAddress.toString}:${sock.getPort} - ${e.getMessage}")
        closeSocketAndCleanPeerMap(sock)
        return false
    }
    true
  }

  /**
    * Close client
    */
  def close() = this.synchronized {
    if (isClosed.getAndSet(true))
      throw new IllegalStateException("Communication Client is closed. Unable to operate.")
    peerMap.foreach { x =>
      try {
        x._2.close()
      } catch {
        case e@(_: ConnectException | _: IOException) =>
          CommunicationClient.logger.warn(s"An exception occurred: ${e.getMessage}")
          throw e
      }
    }
    peerMap.clear()
  }
}

