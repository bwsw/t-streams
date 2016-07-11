package com.bwsw.tstreams.coordination.transactions.transport.impl

import java.util.concurrent.LinkedBlockingQueue

import akka.actor.ActorSystem
import com.bwsw.tstreams.coordination.transactions.messages._
import com.bwsw.tstreams.coordination.transactions.transport.impl.client.TcpIMessageClient
import com.bwsw.tstreams.coordination.transactions.transport.impl.server.IMessageListener
import com.bwsw.tstreams.coordination.transactions.transport.traits.ITransport

/**
 * [[ITransport]] implementation
 */
class TcpTransport(implicit system : ActorSystem) extends ITransport{
  private var listener : IMessageListener = null
  private val sender : TcpIMessageClient = new TcpIMessageClient
  private val msgQueue = new LinkedBlockingQueue[IMessage]()

  /**
   * Request to disable concrete master
 *
   * @param msg Msg to disable master
   * @param timeout Timeout for waiting
   * @return DeleteMasterResponse or null
   */
  override def deleteMasterRequest(msg: DeleteMasterRequest, timeout: Int): IMessage = {
    val response = sender.sendAndWaitResponse(msg, timeout)
    response
  }

  /**
   * Request to figure out state of receiver
 *
   * @param msg Message
   * @return PingResponse or null
   */
  override def pingRequest(msg: PingRequest, timeout: Int): IMessage = {
    val response = sender.sendAndWaitResponse(msg, timeout)
    response
  }

  /**
   * Wait incoming requests(every p2p agent must handle this incoming messages)
 *
   * @return IMessage or null
   */
  override def waitRequest(): IMessage =
    msgQueue.take()

  /**
   * Send empty request (just for testing)
 *
   * @param msg EmptyRequest
   */
  override def stopRequest(msg: EmptyRequest): Unit = {
    sender.sendAndWaitResponse(msg, 3)
  }

  /**
   * Request to set concrete master
 *
   * @param msg Message
   * @param timeout Timeout to wait master
   * @return SetMasterResponse or null
   */
  override def setMasterRequest(msg: SetMasterRequest, timeout: Int): IMessage = {
    val response: IMessage = sender.sendAndWaitResponse(msg, timeout)
    response
  }

  /**
   * Request to get Txn
 *
   * @param msg Message
   * @param timeout Timeout to wait master
   * @return TransactionResponse or null
   */
  override def transactionRequest(msg: TransactionRequest, timeout: Int): IMessage = {
    val response: IMessage = sender.sendAndWaitResponse(msg, timeout)
    response
  }

  /**
   * Send response to requester
 *
   * @param msg IMessage
   */
  override def response(msg: IMessage): Unit = {
    listener.response(msg)
  }

  /**
   * Bind local agent address in transport
   */
  override def bindLocalAddress(address: String): Unit = {
    val splits = address.split(":")
    assert(splits.size == 2)
    val port = splits(1).toInt
    listener = new IMessageListener(port)
    listener.addCallback((msg: IMessage) => {
      msgQueue.add(msg)
    })
    listener.start()
  }

  /**
   * Stop transport listen incoming messages
   */
  override def unbindLocalAddress(): Unit = {
    sender.close()
    listener.stop()
  }

  /**
   * Request to publish event about Txn
 *
   * @param msg Message
   * @param timeout Timeout to wait master
   */
  override def publishRequest(msg: PublishRequest, timeout: Int): IMessage = {
    val response: IMessage = sender.sendAndWaitResponse(msg, timeout)
    response
  }
}