package com.bwsw.tstreams.coordination.producer.transport.traits

import com.bwsw.tstreams.coordination.messages.master._

/**
  * Basic trait for transport
  */
trait ITransport {

  /**
    * Request to disable concrete master
    *
    * @param msg     Msg to disable master
    * @param timeout Timeout for waiting
    */
  def deleteMasterRequest(msg: DeleteMasterRequest, timeout: Int): IMessage

  /**
    * Request to set concrete master
    *
    * @param msg     Message
    * @param timeout Timeout to wait master
    */
  def setMasterRequest(msg: SetMasterRequest, timeout: Int): IMessage

  /**
    * Request to get Txn
    *
    * @param msg     Message
    * @param timeout Timeout to wait master
    */
  def transactionRequest(msg: NewTransactionRequest, timeout: Int): IMessage

  /**
    * Request to publish event about Txn
    *
    * @param msg     Message
    * @param timeout Timeout to wait master
    */
  def publishRequest(msg: PublishRequest, timeout: Int): IMessage

  /**
    * Request to figure out state of receiver
    *
    * @param msg Message
    */
  def pingRequest(msg: PingRequest, timeout: Int): IMessage

  /**
    * Wait incoming requests(every p2p agent must handle this incoming messages)
    */
  def waitRequest(): IMessage

  /**
    * Send response to requester
    *
    * @param msg IMessage
    */
  def respond(msg: IMessage): Unit

  /**
    * Send empty request
    *
    * @param msg EmptyRequest
    */
  def stopRequest(msg: EmptyRequest): Unit

  /**
    * Bind local agent address in transport
    */
  def bindLocalAddress(address: String): Unit

  /**
    * Stop transport listen incoming messages
    */
  def unbindLocalAddress(): Unit
}

