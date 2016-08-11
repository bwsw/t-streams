package com.bwsw.tstreams.coordination.producer.transport.impl

import java.util.concurrent.LinkedBlockingQueue

import com.bwsw.tstreams.common.TimeTracker
import com.bwsw.tstreams.coordination.messages.master._
import com.bwsw.tstreams.coordination.producer.transport.impl.client.InterProducerCommunicationClient
import com.bwsw.tstreams.coordination.producer.transport.impl.server.ProducerRequestsTcpServer
import com.bwsw.tstreams.coordination.producer.transport.traits.ITransport
import org.slf4j.LoggerFactory

/**
  * [[ITransport]] implementation
  */
class TcpTransport(timeoutMs: Int, retryCount: Int = 3, retryDelay: Int = 5) extends ITransport {
  private var server: ProducerRequestsTcpServer = null
  private val client: InterProducerCommunicationClient = new InterProducerCommunicationClient(timeoutMs, retryCount, retryDelay)
  private val msgQueue = new LinkedBlockingQueue[IMessage]()

  /**
    * Request to disable concrete master
    *
    * @param msg     Msg to disable master
    * @return DeleteMasterResponse or null
    */
  override def deleteMasterRequest(msg: DeleteMasterRequest): IMessage = {
    val response = client.sendAndWaitResponse(msg)
    response
  }

  override def getTimeout() = timeoutMs

  /**
    * Request to figure out state of receiver
    *
    * @param msg Message
    * @return PingResponse or null
    */
  override def pingRequest(msg: PingRequest): IMessage = {
    val response = client.sendAndWaitResponse(msg)
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
    val logger = LoggerFactory.getLogger(this.getClass)
    client.sendAndNoWaitResponse(msg)
  }

  /**
    * Request to set concrete master
    *
    * @param msg     Message
    * @return SetMasterResponse or null
    */
  override def setMasterRequest(msg: SetMasterRequest): IMessage = {
    val response: IMessage = client.sendAndWaitResponse(msg)
    response
  }

  /**
    * Request to get Txn
    *
    * @param msg     Message
    * @return TransactionResponse or null
    */
  override def transactionRequest(msg: NewTransactionRequest): IMessage = {
    val response: IMessage = client.sendAndWaitResponse(msg)
    response
  }

  /**
    * Request to publish event about Txn
    *
    * @param msg     Message
    */
  override def publishRequest(msg: PublishRequest): Unit = {
    client.sendAndNoWaitResponse(msg)
  }

  /**
    * Request to publish event about Txn
    *
    * @param msg     Message
    */
  override def materializeRequest(msg: MaterializeRequest): Unit = {
    client.sendAndNoWaitResponse(msg)
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


}