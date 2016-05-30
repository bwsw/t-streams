package com.bwsw.tstreams.coordination.transactions.messages

import java.util.UUID
import com.bwsw.tstreams.coordination.pubsub.messages.ProducerTopicMessage
import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}


@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "type")
@JsonSubTypes(Array(
  new Type(value = classOf[TransactionRequest], name = "TransactionRequest"),
  new Type(value = classOf[TransactionResponse], name = "TransactionResponse"),
  new Type(value = classOf[DeleteMasterRequest], name = "DeleteMasterRequest"),
  new Type(value = classOf[DeleteMasterResponse], name = "DeleteMasterResponse"),
  new Type(value = classOf[SetMasterRequest], name = "SetMasterRequest"),
  new Type(value = classOf[SetMasterResponse], name = "SetMasterResponse"),
  new Type(value = classOf[PingRequest], name = "PingRequest"),
  new Type(value = classOf[PingResponse], name = "PingResponse"),
  new Type(value = classOf[PublishRequest], name = "PublishRequest"),
  new Type(value = classOf[PublishResponse], name = "PublishResponse"),
  new Type(value = classOf[EmptyResponse], name = "EmptyResponse"),
  new Type(value = classOf[EmptyRequest], name = "EmptyRequest")
))
trait IMessage {
  var msgID : String = UUID.randomUUID().toString
  val senderID : String
  val receiverID : String
  val partition : Int
}

case class TransactionRequest(senderID : String, receiverID : String, partition : Int) extends IMessage
case class TransactionResponse(senderID : String, receiverID : String, txnUUID : UUID, partition : Int) extends IMessage

case class DeleteMasterRequest(senderID : String, receiverID : String, partition : Int) extends IMessage
case class DeleteMasterResponse(senderID : String, receiverID : String, partition : Int) extends IMessage

case class SetMasterRequest(senderID : String, receiverID : String, partition : Int) extends IMessage
case class SetMasterResponse(senderID : String, receiverID : String, partition : Int) extends IMessage

case class PingRequest(senderID : String, receiverID : String, partition : Int) extends IMessage
case class PingResponse(senderID : String, receiverID : String, partition : Int) extends IMessage

case class PublishRequest(senderID : String, receiverID : String, msg : ProducerTopicMessage) extends IMessage {
  override val partition: Int = msg.partition
}

case class PublishResponse(senderID : String, receiverID : String, msg : ProducerTopicMessage) extends IMessage {
  override val partition: Int = msg.partition
}


case class EmptyRequest(senderID : String, receiverID : String, partition : Int) extends IMessage
case class EmptyResponse(senderID : String, receiverID : String, partition : Int) extends IMessage

