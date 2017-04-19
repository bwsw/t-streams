package com.bwsw.tstreams.common

import com.bwsw.tstreams.agents.producer.AgentConfiguration
import com.bwsw.tstreams.coordination.messages.master._

import scala.collection.mutable
import scala.util.control.Breaks._

/**
  * TStreams object serializer
  */
object ProtocolMessageSerializer {
  private def serializeInternal(value: Any): String = {
    value match {
      case AgentConfiguration(id, prior, penalty, uniqueID) =>
        s"{AS,$id,$prior,$penalty,$uniqueID}"

      case x: EmptyResponse =>
        s"{ERs,${x.senderID},${x.receiverID},${x.partition},${x.msgID},${x.remotePeerTimestamp}}"

      case x: NewTransactionRequest =>
        s"{TRq,${x.senderID},${x.receiverID},${x.partition},${x.msgID},${x.remotePeerTimestamp}}"

      case x: TransactionResponse =>
        s"{TRs,${x.senderID},${x.receiverID},${x.transactionID.toString},${x.partition},${x.msgID},${x.remotePeerTimestamp}}"

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
      case "ERs" =>
        assert(tokens.size == 6)
        val res = EmptyResponse(tokens(1).toString, tokens(2).toString, tokens(3).toString.toInt)
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
          tokens(2).toString, tokens(3).toString.toLong, tokens(4).toString.toInt)
        res.msgID = tokens(5).toString.toLong
        res.remotePeerTimestamp = tokens(6).toString.toLong
        res
      case "AS" =>
        assert(tokens.size == 5)
        AgentConfiguration(tokens(1).toString, tokens(2).toString.toInt, tokens(3).toString.toInt, tokens(4).toString.toInt)
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

  /**
    * Wrap message with line delimiter to separate it on server side
    *
    * @param msg
    * @return
    */
  def wrapMsg(msg: String): String = {
    msg + "\n"
  }

  class ProtocolMessageSerializerException(msg: String) extends Exception(msg)

}



