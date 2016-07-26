package com.bwsw.tstreams.common.serializer
import java.util.UUID

import com.bwsw.tstreams.common.serializer.TStreamsSerializer.TStreamsSerializerException
import com.bwsw.tstreams.coordination.pubsub.messages.{ProducerTopicMessage, ProducerTransactionStatus}
import com.bwsw.tstreams.coordination.transactions.messages._
import com.bwsw.tstreams.coordination.transactions.peertopeer.AgentSettings

import scala.collection.mutable
import scala.util.control.Breaks._

/**
  * TStreams object serializer
  */
class TStreamsSerializer {
  private def serializeInternal(value: Any): String = {
    value match {
      case AgentSettings(id, prior, penalty) =>
        s"{AS,$id,$prior,$penalty}"

      case x : DeleteMasterRequest =>
        s"{DMRq,${x.senderID},${x.receiverID},${x.partition},${x.msgID}}"

      case x : DeleteMasterResponse =>
        s"{DMRs,${x.senderID},${x.receiverID},${x.partition},${x.msgID}}"

      case x : EmptyRequest =>
        s"{ERq,${x.senderID},${x.receiverID},${x.partition},${x.msgID}}"

      case x : EmptyResponse =>
        s"{ERs,${x.senderID},${x.receiverID},${x.partition},${x.msgID}}"

      case x : PingRequest =>
        s"{PiRq,${x.senderID},${x.receiverID},${x.partition},${x.msgID}}"

      case x : PingResponse =>
        s"{PiRs,${x.senderID},${x.receiverID},${x.partition},${x.msgID}}"

      case x : PublishRequest =>
        val serializedMsg = serializeInternal(x.msg)
        s"{PuRq,${x.senderID},${x.receiverID},$serializedMsg,${x.msgID}}"

      case x : PublishResponse =>
        val serializedMsg = serializeInternal(x.msg)
        s"{PuRs,${x.senderID},${x.receiverID},$serializedMsg,${x.msgID}}"

      case x : SetMasterRequest =>
        s"{SMRq,${x.senderID},${x.receiverID},${x.partition},${x.msgID}}"

      case x : SetMasterResponse =>
        s"{SMRs,${x.senderID},${x.receiverID},${x.partition},${x.msgID}}"

      case x : TransactionRequest =>
        s"{TRq,${x.senderID},${x.receiverID},${x.partition},${x.msgID}}"

      case x : TransactionResponse =>
        s"{TRs,${x.senderID},${x.receiverID},${x.txnUUID.toString},${x.partition},${x.msgID}}"

      case ProducerTopicMessage(txnUuid,ttl,status,partition) =>
        val serializedStatus = serializeInternal(status)
        s"{PTM,${txnUuid.toString},$ttl,$serializedStatus,$partition}"

      case ProducerTransactionStatus.preCheckpoint => s"{P}"
      case ProducerTransactionStatus.`postCheckpoint` => "{F}"
      case ProducerTransactionStatus.`update` => "{U}"
      case ProducerTransactionStatus.`cancel` => "{C}"
      case ProducerTransactionStatus.opened => "{O}"
    }
  }

  private def deserializeToAny(value: String): Any = {
    assert(value.head == '{' && value.last == '}')
    val tokens = mutable.ListBuffer[Any]()
    var temp = ""
    var i = 1
    while (i < value.length - 1){
      val char = value(i)
      char match {
        case ',' =>
          if (temp.nonEmpty)
            tokens += temp
          temp = ""
          i += 1
        case '{' =>
          var pos = -1
          var cntOpen = 0
          breakable {
            for (j <- i + 1 until value.length - 1) {
              if (value(j) == '{')
                cntOpen += 1
              if (cntOpen == 0 && value(j) == '}'){
                pos = j
                break()
              }
              if (value(j) == '}')
                cntOpen -= 1
            }
          }
          assert(pos != -1)
          val token = deserializeToAny(value.substring(i, pos+1))
          tokens += token
          temp = ""
          i = pos + 1
        case _ =>
          temp += char
          i += 1
      }
    }
    assert(temp.nonEmpty)
    tokens += temp

    tokens.head.toString match {
      case "DMRq" =>
        assert(tokens.size == 5)
        val res = DeleteMasterRequest(tokens(1).toString,tokens(2).toString,tokens(3).toString.toInt)
        res.msgID = tokens(4).toString.toLong
        res
      case "DMRs" =>
        assert(tokens.size == 5)
        val res = DeleteMasterResponse(tokens(1).toString,tokens(2).toString,tokens(3).toString.toInt)
        res.msgID = tokens(4).toString.toLong
        res
      case "ERq" =>
        assert(tokens.size == 5)
        val res = EmptyRequest(tokens(1).toString, tokens(2).toString, tokens(3).toString.toInt)
        res.msgID = tokens(4).toString.toLong
        res
      case "ERs" =>
        assert(tokens.size == 5)
        val res = EmptyResponse(tokens(1).toString, tokens(2).toString, tokens(3).toString.toInt)
        res.msgID = tokens(4).toString.toLong
        res
      case "PiRq" =>
        assert(tokens.size == 5)
        val res = PingRequest(tokens(1).toString, tokens(2).toString, tokens(3).toString.toInt)
        res.msgID = tokens(4).toString.toLong
        res
      case "PiRs" =>
        assert(tokens.size == 5)
        val res = PingResponse(tokens(1).toString, tokens(2).toString, tokens(3).toString.toInt)
        res.msgID = tokens(4).toString.toLong
        res
      case "PuRq" =>
        assert(tokens.size == 5)
        val res = PublishRequest(tokens(1).toString, tokens(2).toString, tokens(3).asInstanceOf[ProducerTopicMessage])
        res.msgID = tokens(4).toString.toLong
        res
      case "PuRs" =>
        assert(tokens.size == 5)
        val res = PublishResponse(tokens(1).toString, tokens(2).toString, tokens(3).asInstanceOf[ProducerTopicMessage])
        res.msgID = tokens(4).toString.toLong
        res
      case "SMRq" =>
        assert(tokens.size == 5)
        val res = SetMasterRequest(tokens(1).toString, tokens(2).toString, tokens(3).toString.toInt)
        res.msgID = tokens(4).toString.toLong
        res
      case "SMRs" =>
        assert(tokens.size == 5)
        val res = SetMasterResponse(tokens(1).toString, tokens(2).toString, tokens(3).toString.toInt)
        res.msgID = tokens(4).toString.toLong
        res
      case "TRq" =>
        assert(tokens.size == 5)
        val res = TransactionRequest(tokens(1).toString, tokens(2).toString, tokens(3).toString.toInt)
        res.msgID = tokens(4).toString.toLong
        res
      case "TRs" =>
        assert(tokens.size == 6)
        val res = TransactionResponse(tokens(1).toString,
          tokens(2).toString, UUID.fromString(tokens(3).toString), tokens(4).toString.toInt)
        res.msgID = tokens(5).toString.toLong
        res
      case "PTM" =>
        assert(tokens.size == 5)
        ProducerTopicMessage(UUID.fromString(tokens(1).toString), tokens(2).toString.toInt,
          tokens(3).asInstanceOf[ProducerTransactionStatus.ProducerTransactionStatus], tokens(4).toString.toInt)
      case "AS" =>
        assert(tokens.size == 4)
        AgentSettings(tokens(1).toString, tokens(2).toString.toInt, tokens(3).toString.toInt)
      case "P" =>
        assert(tokens.size == 1)
        ProducerTransactionStatus.preCheckpoint
      case "F" =>
        assert(tokens.size == 1)
        ProducerTransactionStatus.postCheckpoint
      case "U" =>
        assert(tokens.size == 1)
        ProducerTransactionStatus.update
      case "C" =>
        assert(tokens.size == 1)
        ProducerTransactionStatus.cancel
      case "O" =>
        assert(tokens.size == 1)
        ProducerTransactionStatus.opened
    }
  }

  def serialize(value : Any) : String = {
    try {
      serializeInternal(value)
    }
    catch {
      case e : Exception =>
        throw new TStreamsSerializerException(s"msg : {${e.getMessage}} for value : {$value}")
    }
  }

  def deserialize[T](value : String) : T = {
    try {
      val any = deserializeToAny(value)
      any.asInstanceOf[T]
    }
    catch {
      case e : Exception =>
        throw new TStreamsSerializerException(s"msg : {${e.getMessage}} for value : {$value}")
    }
  }
}


object TStreamsSerializer{
  class TStreamsSerializerException(msg : String) extends Exception(msg)
}
