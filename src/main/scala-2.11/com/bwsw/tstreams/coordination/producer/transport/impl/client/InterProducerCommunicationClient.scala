package com.bwsw.tstreams.coordination.producer.transport.impl.client

import java.io.{BufferedReader, IOException, InputStreamReader}
import java.net.{Socket, SocketTimeoutException}
import java.util.concurrent.atomic.AtomicBoolean
import com.bwsw.tstreams.common.{TimeTracker, ProtocolMessageSerializer}
import com.bwsw.tstreams.common.ProtocolMessageSerializer.ProtocolMessageSerializerException
import com.bwsw.tstreams.coordination.messages.master.IMessage
import org.slf4j.LoggerFactory

import scala.collection.mutable

object InterProducerCommunicationClient {
 val logger = LoggerFactory.getLogger(this.getClass)
}

/**
  * Client for sending [[IMessage]]]
  */
class InterProducerCommunicationClient(timeoutMs: Int, retryCount: Int = 3, retryDelay: Int = 5) {
  private val peerMap = mutable.Map[String, Socket]()
  private val serializer = new ProtocolMessageSerializer
  private val isClosed = new AtomicBoolean(false)

  private def openSocket(msg: IMessage): Socket = {
    try {
      InterProducerCommunicationClient.logger.info(s"Start opening socket from peer ${msg.senderID} to peer ${msg.receiverID}.")
      val splits = msg.receiverID.split(":")
      assert(splits.size == 2)
      val host = splits(0)
      val port = splits(1).toInt
      val sock = new Socket(host, port)
      InterProducerCommunicationClient.logger.info(s"Opened socket from peer ${msg.senderID} to peer ${msg.receiverID}.")
      sock
    } catch {
      case e: IOException =>
        InterProducerCommunicationClient.logger.warn(s"exception occurred: ${e.getMessage}")
        InterProducerCommunicationClient.logger.warn(msg.toString())
        throw e
    }
  }

  private def getSocket(msg: IMessage): Socket = {
    var socket: Socket = null
    this.synchronized {
      if (peerMap.contains(msg.receiverID)) {
        if (InterProducerCommunicationClient.logger.isDebugEnabled)
          InterProducerCommunicationClient.logger.debug(s"Socket from peer ${msg.senderID} to peer ${msg.receiverID} is already known.")
        socket = peerMap(msg.receiverID)
        if (socket.isClosed || !socket.isConnected || socket.isOutputShutdown) {
          InterProducerCommunicationClient.logger.info(s"Socket from peer ${msg.senderID} to peer ${msg.receiverID} is in wrong state.")
          socket = openSocket(msg)
          socket.setSoTimeout(timeoutMs)
          socket.setTcpNoDelay(true)
          peerMap(msg.receiverID) = socket
        }
      } else {
        InterProducerCommunicationClient.logger.info(s"Socket from peer ${msg.senderID} to peer ${msg.receiverID} is not known.")
        socket = openSocket(msg)
        socket.setSoTimeout(timeoutMs)
        peerMap(msg.receiverID) = socket
      }
    }
    socket
  }

  private def withRetryDo[TYPE](failDeterminant: TYPE, f: () => TYPE, cnt: Int): TYPE = {
    if(cnt == 0)
      throw new IllegalStateException(s"Operation is failed to complete in ${cnt} retries.")
    val rv = f()
    if(failDeterminant == rv) {
      InterProducerCommunicationClient.logger.warn(s"Operation failed. Retry it for ${cnt} times more.")
      Thread.sleep(retryDelay * 1000)
      withRetryDo[TYPE](failDeterminant, f, cnt - 1)
    }
    else
      rv
  }

  /**
    * @param msg     Message to send
    * @return Response message
    */
  def sendAndWaitResponse(msg: IMessage): IMessage = {
    if(isClosed.get)
      throw new IllegalStateException("Communication Client is closed. Unable to operate.")
    withRetryDo[IMessage](null, () => {
      val sock = getSocket(msg)
      val r = writeMsgAndWaitResponse(sock, msg)
      r}, retryCount)
  }



  /**
    * @param msg     Message to send
    * @return Response message
    */
  def sendAndNoWaitResponse(msg: IMessage):Unit = {
    if(isClosed.get)
      throw new IllegalStateException("Communication Client is closed. Unable to operate.")
    withRetryDo[Boolean](false, () => {
      val sock = getSocket(msg)
      writeMsgAndNoWaitResponse(sock, msg)
    }, retryCount)
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
  private def closeSocketAndCleanPeerMap(socket: Socket, msg: IMessage) = this.synchronized {
    try {
      InterProducerCommunicationClient.logger.info(s"Socket from peer ${msg.senderID} to peer ${msg.receiverID} is to be closed.")
      socket.close()
      InterProducerCommunicationClient.logger.info(s"Socket from peer ${msg.senderID} to peer ${msg.receiverID} is closed.")
    } catch {
      case e: IOException =>
        InterProducerCommunicationClient.logger.warn(s"exception occurred: ${e.getMessage}")
        InterProducerCommunicationClient.logger.warn(msg.toString())
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
    val reqString = wrapMsg(serializer.serialize(msg))
    try {
      if (InterProducerCommunicationClient.logger.isDebugEnabled)
        InterProducerCommunicationClient.logger.debug(s"To send message ${reqString} from peer ${msg.senderID} to peer ${msg.receiverID}.")
      val outputStream = sock.getOutputStream
      outputStream.write(reqString.getBytes)
      outputStream.flush()
      if (InterProducerCommunicationClient.logger.isDebugEnabled)
        InterProducerCommunicationClient.logger.debug(s"Sent message ${reqString} from peer ${msg.senderID} to peer ${msg.receiverID}.")
    }
    catch {
      case e: IOException =>
        InterProducerCommunicationClient.logger.warn(s"exception occurred: ${e.getMessage}")
        InterProducerCommunicationClient.logger.warn(msg.toString())
        closeSocketAndCleanPeerMap(sock, msg)
        return null.asInstanceOf[IMessage]
    }
    //wait response with timeout
    var answer = {
      try {
        if (InterProducerCommunicationClient.logger.isDebugEnabled)
          InterProducerCommunicationClient.logger.debug(s"To receive response message on ${reqString}sent from peer ${msg.senderID} to peer ${msg.receiverID}.")
        val reader = new BufferedReader(new InputStreamReader(sock.getInputStream))
        val string = reader.readLine()
        if (string == null)
          null.asInstanceOf[IMessage]
        else {
          val response = serializer.deserialize[IMessage](string)
          if (InterProducerCommunicationClient.logger.isDebugEnabled)
            InterProducerCommunicationClient.logger.debug(s"Received response message ${response} on ${reqString}sent from peer ${msg.senderID} to peer ${msg.receiverID}.")
          response
        }
      }
      catch {
        case e@(_: SocketTimeoutException | _: ProtocolMessageSerializerException | _: IOException) =>
          InterProducerCommunicationClient.logger.warn(s"exception occurred: ${e.getMessage}")
          InterProducerCommunicationClient.logger.warn(msg.toString())
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
    val string = wrapMsg(serializer.serialize(msg))
    try {
      if (InterProducerCommunicationClient.logger.isDebugEnabled)
        InterProducerCommunicationClient.logger.debug(s"To send message ${string} from peer ${msg.senderID} to peer ${msg.receiverID}.")
      val outputStream = sock.getOutputStream
      outputStream.write(string.getBytes)
      outputStream.flush()
      if (InterProducerCommunicationClient.logger.isDebugEnabled)
        InterProducerCommunicationClient.logger.debug(s"Sent message ${string} from peer ${msg.senderID} to peer ${msg.receiverID}.")
    }
    catch {
      case e: IOException =>
        InterProducerCommunicationClient.logger.warn(s"exception occurred: ${e.getMessage}")
        InterProducerCommunicationClient.logger.warn(msg.toString())
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
          InterProducerCommunicationClient.logger.warn(s"exception occurred: ${e.getMessage}")
          throw e
      }
    }
    peerMap.clear()
  }
}

