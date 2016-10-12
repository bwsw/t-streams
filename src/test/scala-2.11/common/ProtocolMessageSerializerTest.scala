package common

import com.bwsw.tstreams.agents.producer.AgentConfiguration
import com.bwsw.tstreams.common.ProtocolMessageSerializer
import com.bwsw.tstreams.coordination.messages.master._
import com.bwsw.tstreams.coordination.messages.state.{TransactionStateMessage, TransactionStatus}
import org.scalatest.{FlatSpec, Matchers}
import testutils.LocalGeneratorCreator


class ProtocolMessageSerializerTest extends FlatSpec with Matchers {

  "TStreams serializer" should "serialize and deserialize EmptyRequest" in {
    val clazz = EmptyRequest("snd", "rcv", 0)
    val string = ProtocolMessageSerializer.serialize(clazz)
    val req = ProtocolMessageSerializer.deserialize[EmptyRequest](string)
    clazz shouldEqual req
  }
  "TStreams serializer" should "serialize and deserialize EmptyResponse" in {
    val clazz = EmptyResponse("snd", "rcv", 0)
    val string = ProtocolMessageSerializer.serialize(clazz)
    val req = ProtocolMessageSerializer.deserialize[EmptyResponse](string)
    clazz shouldEqual req
  }

  "TStreams serializer" should "serialize and deserialize PublishRequest" in {
    val clazz = PublishRequest("snd", "rcv", TransactionStateMessage(LocalGeneratorCreator.getTransaction(), 228, TransactionStatus.cancel, 1488, 1, 0, 2))
    val string = ProtocolMessageSerializer.serialize(clazz)
    val req = ProtocolMessageSerializer.deserialize[PublishRequest](string)
    clazz shouldEqual req
  }
  "TStreams serializer" should "serialize and deserialize PublishResponse" in {
    val clazz = PublishResponse("snd", "rcv", TransactionStateMessage(LocalGeneratorCreator.getTransaction(), 228, TransactionStatus.cancel, 1488, 1, 0, 2))
    val string = ProtocolMessageSerializer.serialize(clazz)
    val req = ProtocolMessageSerializer.deserialize[PublishResponse](string)
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
    val clazz = TransactionStateMessage(LocalGeneratorCreator.getTransaction(), 123, TransactionStatus.postCheckpoint, 5, 1, 2, 3)
    val string = ProtocolMessageSerializer.serialize(clazz)
    val req = ProtocolMessageSerializer.deserialize[TransactionStateMessage](string)
    clazz shouldEqual req
  }
  "TStreams serializer" should "serialize and deserialize PTS" in {
    val pre = TransactionStatus.preCheckpoint
    val fin = TransactionStatus.postCheckpoint
    val canceled = TransactionStatus.cancel
    val updated = TransactionStatus.update
    val opened = TransactionStatus.opened
    val materialize = TransactionStatus.materialize
    val invalid = TransactionStatus.invalid

    List(pre, fin, canceled, updated, opened, materialize, invalid).foreach(i =>
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
