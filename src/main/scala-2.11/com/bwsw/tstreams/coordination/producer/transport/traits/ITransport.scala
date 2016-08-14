package com.bwsw.tstreams.coordination.producer.transport.traits

import com.bwsw.tstreams.coordination.messages.master._
import io.netty.channel.Channel
import io.netty.channel.socket.SocketChannel

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
    * Send empty request
    *
    * @param msg EmptyRequest
    */
  def stopRequest(msg: EmptyRequest): Unit

  /**
    * Bind local agent address in transport
    */
  def start(callback: (Channel,String) => Unit): Unit

  /**
    * Stop transport listen incoming messages
    */
  def stop(): Unit
}

