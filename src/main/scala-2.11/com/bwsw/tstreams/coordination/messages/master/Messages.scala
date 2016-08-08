package com.bwsw.tstreams.coordination.messages.master

import java.util.UUID

import com.bwsw.tstreams.coordination.messages.state.Message

import scala.util.Random

/**
  * Messages which used for providing
  * interaction between [[com.bwsw.tstreams.coordination.producer.p2p.PeerToPeerAgent]]]
  */
trait IMessage {
  var msgID: Long = Random.nextLong()
  val senderID: String
  val receiverID: String
  val partition: Int
}

case class TransactionRequest(senderID: String, receiverID: String, partition: Int) extends IMessage

case class TransactionResponse(senderID: String, receiverID: String, txnUUID: UUID, partition: Int) extends IMessage

case class DeleteMasterRequest(senderID: String, receiverID: String, partition: Int) extends IMessage

case class DeleteMasterResponse(senderID: String, receiverID: String, partition: Int) extends IMessage

case class SetMasterRequest(senderID: String, receiverID: String, partition: Int) extends IMessage

case class SetMasterResponse(senderID: String, receiverID: String, partition: Int) extends IMessage

case class PingRequest(senderID: String, receiverID: String, partition: Int) extends IMessage {
}

case class PingResponse(senderID: String, receiverID: String, partition: Int) extends IMessage

case class PublishRequest(senderID: String, receiverID: String, msg: Message) extends IMessage {
  override val partition: Int = msg.partition
}

case class PublishResponse(senderID: String, receiverID: String, msg: Message) extends IMessage {
  override val partition: Int = msg.partition
}


case class EmptyRequest(senderID: String, receiverID: String, partition: Int) extends IMessage

case class EmptyResponse(senderID: String, receiverID: String, partition: Int) extends IMessage

