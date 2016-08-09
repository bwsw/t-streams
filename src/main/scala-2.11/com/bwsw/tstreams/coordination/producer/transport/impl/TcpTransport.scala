package com.bwsw.tstreams.coordination.producer.transport.impl

import java.util.concurrent.LinkedBlockingQueue

import com.bwsw.tstreams.coordination.messages.master._
import com.bwsw.tstreams.coordination.producer.transport.impl.client.TransactionStateUpdateClient
import com.bwsw.tstreams.coordination.producer.transport.impl.server.ProducerRequestsTcpServer
import com.bwsw.tstreams.coordination.producer.transport.traits.ITransport

/**
  * [[ITransport]] implementation
  */
class TcpTransport extends ITransport {
  private var server: ProducerRequestsTcpServer = null
  private val client: TransactionStateUpdateClient = new TransactionStateUpdateClient
  private val msgQueue = new LinkedBlockingQueue[IMessage]()

  /**
    * Request to disable concrete master
    *
    * @param msg     Msg to disable master
    * @param timeout Timeout for waiting
    * @return DeleteMasterResponse or null
    */
  override def deleteMasterRequest(msg: DeleteMasterRequest, timeout: Int): IMessage = {
    val response = client.sendAndWaitResponse(msg, timeout)
    response
  }

  /**
    * Request to figure out state of receiver
    *
    * @param msg Message
    * @return PingResponse or null
    */
  override def pingRequest(msg: PingRequest, timeout: Int): IMessage = {
    val response = client.sendAndWaitResponse(msg, timeout)
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
    client.sendAndWaitResponse(msg, 3)
  }

  /**
    * Request to set concrete master
    *
    * @param msg     Message
    * @param timeout Timeout to wait master
    * @return SetMasterResponse or null
    */
  override def setMasterRequest(msg: SetMasterRequest, timeout: Int): IMessage = {
    val response: IMessage = client.sendAndWaitResponse(msg, timeout)
    response
  }

  /**
    * Request to get Txn
    *
    * @param msg     Message
    * @param timeout Timeout to wait master
    * @return TransactionResponse or null
    */
  override def transactionRequest(msg: NewTransactionRequest, timeout: Int): IMessage = {
    val response: IMessage = client.sendAndWaitResponse(msg, timeout)
    response
  }

  /**
    * Send response to requester
    *
    * @param msg IMessage
    */
  override def respond(msg: IMessage): Unit = {
    server.respond(msg)
  }

  /**
    * Bind local agent address in transport
    */
  override def bindLocalAddress(address: String): Unit = {
    val splits = address.split(":")
    assert(splits.size == 2)
    val port = splits(1).toInt
    server = new ProducerRequestsTcpServer(port)
    server.addCallback((msg: IMessage) => {
      msgQueue.add(msg)
    })
    server.start()
  }

  /**
    * Stop transport listen incoming messages
    */
  override def unbindLocalAddress(): Unit = {
    client.close()
    server.stop()
  }

  /**
    * Request to publish event about Txn
    *
    * @param msg     Message
    * @param timeout Timeout to wait master
    */
  override def publishRequest(msg: PublishRequest, timeout: Int): IMessage = {
    val response: IMessage = client.sendAndWaitResponse(msg, timeout)
    response
  }
}