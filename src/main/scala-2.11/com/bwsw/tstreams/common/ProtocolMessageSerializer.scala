package com.bwsw.tstreams.common

import java.util.UUID

import ProtocolMessageSerializer.ProtocolMessageSerializerException
import com.bwsw.tstreams.coordination.messages.master._
import com.bwsw.tstreams.coordination.messages.state.{Message, TransactionStatus}
import com.bwsw.tstreams.coordination.producer.p2p.AgentSettings

import scala.collection.mutable
import scala.util.control.Breaks._

/**
  * TStreams object serializer
  */
class ProtocolMessageSerializer {
  private def serializeInternal(value: Any): String = {
    value match {
      case AgentSettings(id, prior, penalty) =>
        s"{AS,$id,$prior,$penalty}"

      case x: DeleteMasterRequest =>
        s"{DMRq,${x.senderID},${x.receiverID},${x.partition},${x.msgID},${x.remotePeerTimestamp}}"

      case x: DeleteMasterResponse =>
        s"{DMRs,${x.senderID},${x.receiverID},${x.partition},${x.msgID},${x.remotePeerTimestamp}}"

      case x: EmptyRequest =>
        s"{ERq,${x.senderID},${x.receiverID},${x.partition},${x.msgID},${x.remotePeerTimestamp}}"

      case x: EmptyResponse =>
        s"{ERs,${x.senderID},${x.receiverID},${x.partition},${x.msgID},${x.remotePeerTimestamp}}"

      case x: PingRequest =>
        s"{PiRq,${x.senderID},${x.receiverID},${x.partition},${x.msgID},${x.remotePeerTimestamp}}"

      case x: PingResponse =>
        s"{PiRs,${x.senderID},${x.receiverID},${x.partition},${x.msgID},${x.remotePeerTimestamp}}"

      case x: PublishRequest =>
        val serializedMsg = serializeInternal(x.msg)
        s"{PuRq,${x.senderID},${x.receiverID},$serializedMsg,${x.msgID},${x.remotePeerTimestamp}}"

      case x: MaterializeRequest =>
        val serializedMsg = serializeInternal(x.msg)
        s"{MRq,${x.senderID},${x.receiverID},$serializedMsg,${x.msgID},${x.remotePeerTimestamp}}"

      case x: PublishResponse =>
        val serializedMsg = serializeInternal(x.msg)
        s"{PuRs,${x.senderID},${x.receiverID},$serializedMsg,${x.msgID},${x.remotePeerTimestamp}}"

      case x: SetMasterRequest =>
        s"{SMRq,${x.senderID},${x.receiverID},${x.partition},${x.msgID},${x.remotePeerTimestamp}}"

      case x: SetMasterResponse =>
        s"{SMRs,${x.senderID},${x.receiverID},${x.partition},${x.msgID},${x.remotePeerTimestamp}}"

      case x: NewTransactionRequest =>
        s"{TRq,${x.senderID},${x.receiverID},${x.partition},${x.msgID},${x.remotePeerTimestamp}}"

      case x: TransactionResponse =>
        s"{TRs,${x.senderID},${x.receiverID},${x.txnUUID.toString},${x.partition},${x.msgID},${x.remotePeerTimestamp}}"

      case Message(txnUuid, ttl, status, partition) =>
        val serializedStatus = serializeInternal(status)
        s"{PTM,${txnUuid.toString},$ttl,$serializedStatus,$partition}"

      case TransactionStatus.preCheckpoint => s"{P}"
      case TransactionStatus.`postCheckpoint` => "{F}"
      case TransactionStatus.`update` => "{U}"
      case TransactionStatus.`cancel` => "{C}"
      case TransactionStatus.opened => "{O}"
      case TransactionStatus.materialize => "{M}"
    }
  }

  private def deserializeToAny(value: String): Any = {
    assert(value.head == '{' && value.last == '}')
    val tokens = mutable.ListBuffer[Any]()
    var temp = ""
    var i = 1
    while (i < value.length - 1) {
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
              if (cntOpen == 0 && value(j) == '}') {
                pos = j
                break()
              }
              if (value(j) == '}')
                cntOpen -= 1
            }
          }
          assert(pos != -1)
          val token = deserializeToAny(value.substring(i, pos + 1))
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
        assert(tokens.size == 6)
        val res = DeleteMasterRequest(tokens(1).toString, tokens(2).toString, tokens(3).toString.toInt)
        res.msgID = tokens(4).toString.toLong
        res.remotePeerTimestamp = tokens(5).toString.toLong
        res
      case "DMRs" =>
        assert(tokens.size == 6)
        val res = DeleteMasterResponse(tokens(1).toString, tokens(2).toString, tokens(3).toString.toInt)
        res.msgID = tokens(4).toString.toLong
        res.remotePeerTimestamp = tokens(5).toString.toLong
        res
      case "ERq" =>
        assert(tokens.size == 6)
        val res = EmptyRequest(tokens(1).toString, tokens(2).toString, tokens(3).toString.toInt)
        res.msgID = tokens(4).toString.toLong
        res.remotePeerTimestamp = tokens(5).toString.toLong
        res
      case "ERs" =>
        assert(tokens.size == 6)
        val res = EmptyResponse(tokens(1).toString, tokens(2).toString, tokens(3).toString.toInt)
        res.msgID = tokens(4).toString.toLong
        res.remotePeerTimestamp = tokens(5).toString.toLong
        res
      case "PiRq" =>
        assert(tokens.size == 6)
        val res = PingRequest(tokens(1).toString, tokens(2).toString, tokens(3).toString.toInt)
        res.msgID = tokens(4).toString.toLong
        res.remotePeerTimestamp = tokens(5).toString.toLong
        res
      case "PiRs" =>
        assert(tokens.size == 6)
        val res = PingResponse(tokens(1).toString, tokens(2).toString, tokens(3).toString.toInt)
        res.msgID = tokens(4).toString.toLong
        res.remotePeerTimestamp = tokens(5).toString.toLong
        res
      case "PuRq" =>
        assert(tokens.size == 6)
        val res = PublishRequest(tokens(1).toString, tokens(2).toString, tokens(3).asInstanceOf[Message])
        res.msgID = tokens(4).toString.toLong
        res.remotePeerTimestamp = tokens(5).toString.toLong
        res
      case "MRq" =>
        assert(tokens.size == 6)
        val res = MaterializeRequest(tokens(1).toString, tokens(2).toString, tokens(3).asInstanceOf[Message])
        res.msgID = tokens(4).toString.toLong
        res.remotePeerTimestamp = tokens(5).toString.toLong
        res

      case "PuRs" =>
        assert(tokens.size == 6)
        val res = PublishResponse(tokens(1).toString, tokens(2).toString, tokens(3).asInstanceOf[Message])
        res.msgID = tokens(4).toString.toLong
        res.remotePeerTimestamp = tokens(5).toString.toLong
        res
      case "SMRq" =>
        assert(tokens.size == 6)
        val res = SetMasterRequest(tokens(1).toString, tokens(2).toString, tokens(3).toString.toInt)
        res.msgID = tokens(4).toString.toLong
        res.remotePeerTimestamp = tokens(5).toString.toLong
        res
      case "SMRs" =>
        assert(tokens.size == 6)
        val res = SetMasterResponse(tokens(1).toString, tokens(2).toString, tokens(3).toString.toInt)
        res.msgID = tokens(4).toString.toLong
        res.remotePeerTimestamp = tokens(5).toString.toLong
        res
      case "TRq" =>
        assert(tokens.size == 6)
        val res = NewTransactionRequest(tokens(1).toString, tokens(2).toString, tokens(3).toString.toInt)
        res.msgID = tokens(4).toString.toLong
        res.remotePeerTimestamp = tokens(5).toString.toLong
        res
      case "TRs" =>
        assert(tokens.size == 7)
        val res = TransactionResponse(tokens(1).toString,
          tokens(2).toString, UUID.fromString(tokens(3).toString), tokens(4).toString.toInt)
        res.msgID = tokens(5).toString.toLong
        res.remotePeerTimestamp = tokens(6).toString.toLong
        res
      case "PTM" =>
        assert(tokens.size == 5)
        Message(UUID.fromString(tokens(1).toString), tokens(2).toString.toInt,
          tokens(3).asInstanceOf[TransactionStatus.ProducerTransactionStatus], tokens(4).toString.toInt)
      case "AS" =>
        assert(tokens.size == 4)
        AgentSettings(tokens(1).toString, tokens(2).toString.toInt, tokens(3).toString.toInt)
      case "P" =>
        assert(tokens.size == 1)
        TransactionStatus.preCheckpoint
      case "F" =>
        assert(tokens.size == 1)
        TransactionStatus.postCheckpoint
      case "U" =>
        assert(tokens.size == 1)
        TransactionStatus.update
      case "C" =>
        assert(tokens.size == 1)
        TransactionStatus.cancel
      case "O" =>
        assert(tokens.size == 1)
        TransactionStatus.opened
      case "M" =>
        assert(tokens.size == 1)
        TransactionStatus.materialize
    }
  }

  def serialize(value: Any): String = {
    try {
      serializeInternal(value)
    }
    catch {
      case e: Exception =>
        throw new ProtocolMessageSerializerException(s"msg : {${e.getMessage}} for value : {$value}")
    }
  }

  def deserialize[T](value: String): T = {
    try {
      val any = deserializeToAny(value)
      any.asInstanceOf[T]
    }
    catch {
      case e: Exception =>
        throw new ProtocolMessageSerializerException(s"msg : {${e.getMessage}} for value : {$value}")
    }
  }
}


object ProtocolMessageSerializer {

  class ProtocolMessageSerializerException(msg: String) extends Exception(msg)

}
