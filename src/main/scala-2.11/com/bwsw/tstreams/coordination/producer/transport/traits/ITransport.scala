package com.bwsw.tstreams.coordination.producer.transport.traits

import com.bwsw.tstreams.coordination.messages.master._

/**
  * Basic trait for transport
  */
trait ITransport {

  def getTimeout(): Int
  /**
    * Request to disable concrete master
    *
    * @param msg     Msg to disable master
    */
  def deleteMasterRequest(msg: DeleteMasterRequest): IMessage

  /**
    * Request to set concrete master
    *
    * @param msg     Message
    */
  def setMasterRequest(msg: SetMasterRequest): IMessage

  /**
    * Request to get Txn
    *
    * @param msg     Message
    */
  def transactionRequest(msg: NewTransactionRequest): IMessage

  /**
    * Request to publish event about Txn
    *
    * @param msg     Message
    */
  def publishRequest(msg: PublishRequest): Unit

  /**
    * Request to materialize event about Txn
    *
    * @param msg     Message
    */
  def materializeRequest(msg: MaterializeRequest): Unit


  /**
    * Request to figure out state of receiver
    *
    * @param msg Message
    */
  def pingRequest(msg: PingRequest): IMessage

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

