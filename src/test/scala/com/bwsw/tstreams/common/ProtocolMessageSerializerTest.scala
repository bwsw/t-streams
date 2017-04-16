package com.bwsw.tstreams.common

import com.bwsw.tstreams.agents.producer.AgentConfiguration
import com.bwsw.tstreams.coordination.messages.master._
import com.bwsw.tstreams.coordination.messages.state.{TransactionStateMessage, TransactionStatus}
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
  "TStreams serializer" should "serialize and deserialize PTM" in {
    val clazz = TransactionStateMessage(LocalGeneratorCreator.getTransaction(), 17179869184L, TransactionStatus.checkpointed, 5, 1, 2, 3)
    val string = ProtocolMessageSerializer.serialize(clazz)
    val req = ProtocolMessageSerializer.deserialize[TransactionStateMessage](string)
    clazz shouldEqual req
  }
  "TStreams serializer" should "serialize and deserialize PTS" in {
    val fin = TransactionStatus.checkpointed
    val canceled = TransactionStatus.cancel
    val updated = TransactionStatus.update
    val opened = TransactionStatus.opened
    val materialize = TransactionStatus.materialize
    val invalid = TransactionStatus.invalid

    List(fin, canceled, updated, opened, materialize, invalid).foreach(i =>
      assert(ProtocolMessageSerializer
        .deserialize[TransactionStatus.ProducerTransactionStatus](ProtocolMessageSerializer.serialize(i)) == i))
  }

  "TStreams serializer" should "serialize and deserialize AgentSettings" in {
    val clazz = new AgentConfiguration("agent", 21212, 12121212, 22)
    val string = ProtocolMessageSerializer.serialize(clazz)
    val req = ProtocolMessageSerializer.deserialize[AgentConfiguration](string)
    clazz shouldEqual req
  }
}
