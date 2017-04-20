package com.bwsw.tstreams.common

import com.bwsw.tstreams.coordination.messages.master._
import com.bwsw.tstreams.testutils.LocalGeneratorCreator
import org.scalatest.{FlatSpec, Matchers}


class ProtocolMessageSerializerTest extends FlatSpec with Matchers {

  "TStreams serializer" should "serialize and deserialize EmptyResponse" in {
    val clazz = EmptyResponse("snd", "rcv", 0)
    val string = ProtocolMessageSerializer.serialize(clazz)
    val req = ProtocolMessageSerializer.deserialize[EmptyResponse](string)
    clazz shouldEqual req
  }

  "TStreams serializer" should "serialize and deserialize TransactionRequest" in {
    val clazz = NewTransactionRequest("snd", "rcv", -1)
    val string = ProtocolMessageSerializer.serialize(clazz)
    val req = ProtocolMessageSerializer.deserialize[NewTransactionRequest](string)
    clazz shouldEqual req
  }
  "TStreams serializer" should "serialize and deserialize TransactionResponse" in {
    val clazz = TransactionResponse("snd", "rcv", LocalGeneratorCreator.getTransaction(), 228)
    val string = ProtocolMessageSerializer.serialize(clazz)
    val req = ProtocolMessageSerializer.deserialize[TransactionResponse](string)
    clazz shouldEqual req
  }

}
