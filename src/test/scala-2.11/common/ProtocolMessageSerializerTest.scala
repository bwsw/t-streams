package common

import java.util.UUID

import com.bwsw.tstreams.common.ProtocolMessageSerializer
import com.bwsw.tstreams.coordination.messages.master._
import com.bwsw.tstreams.coordination.messages.state.{TransactionStateMessage, TransactionStatus}
import com.bwsw.tstreams.coordination.producer.AgentSettings
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.TestUtils


class ProtocolMessageSerializerTest extends FlatSpec with Matchers {

  "TStreams serializer" should "serialize and deserialize DeleteMasterRequest" in {
    val clazz = DeleteMasterRequest("snd", "rcv", 0)
    val string = ProtocolMessageSerializer.serialize(clazz)
    val req = ProtocolMessageSerializer.deserialize[DeleteMasterRequest](string)
    clazz shouldEqual req
  }
  "TStreams serializer" should "serialize and deserialize DeleteMasterResponse" in {
    val clazz = DeleteMasterResponse("snd", "rcv", 0)
    val string = ProtocolMessageSerializer.serialize(clazz)
    val req = ProtocolMessageSerializer.deserialize[DeleteMasterResponse](string)
    clazz shouldEqual req
  }
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
  "TStreams serializer" should "serialize and deserialize PingRequest" in {
    val clazz = PingRequest("snd", "rcv", 0)
    val string = ProtocolMessageSerializer.serialize(clazz)
    val req = ProtocolMessageSerializer.deserialize[PingRequest](string)
    clazz shouldEqual req
  }
  "TStreams serializer" should "serialize and deserialize PingResponse" in {
    val clazz = PingResponse("snd", "rcv", 0)
    val string = ProtocolMessageSerializer.serialize(clazz)
    val req = ProtocolMessageSerializer.deserialize[PingResponse](string)
    clazz shouldEqual req
  }
  "TStreams serializer" should "serialize and deserialize PublishRequest" in {
    val clazz = PublishRequest("snd", "rcv", TransactionStateMessage(UUID.randomUUID(), 228, TransactionStatus.cancel, 1488, 1, 0))
    val string = ProtocolMessageSerializer.serialize(clazz)
    val req = ProtocolMessageSerializer.deserialize[PublishRequest](string)
    clazz shouldEqual req
  }
  "TStreams serializer" should "serialize and deserialize PublishResponse" in {
    val clazz = PublishResponse("snd", "rcv", TransactionStateMessage(UUID.randomUUID(), 228, TransactionStatus.cancel, 1488, 1, 0))
    val string = ProtocolMessageSerializer.serialize(clazz)
    val req = ProtocolMessageSerializer.deserialize[PublishResponse](string)
    clazz shouldEqual req
  }
  "TStreams serializer" should "serialize and deserialize SetMasterRequest" in {
    val clazz = SetMasterRequest("snd", "rcv", -1)
    val string = ProtocolMessageSerializer.serialize(clazz)
    val req = ProtocolMessageSerializer.deserialize[SetMasterRequest](string)
    clazz shouldEqual req
  }
  "TStreams serializer" should "serialize and deserialize SetMasterResponse" in {
    val clazz = SetMasterResponse("snd", "rcv", -1)
    val string = ProtocolMessageSerializer.serialize(clazz)
    val req = ProtocolMessageSerializer.deserialize[SetMasterResponse](string)
    clazz shouldEqual req
  }
  "TStreams serializer" should "serialize and deserialize TransactionRequest" in {
    val clazz = NewTransactionRequest("snd", "rcv", -1)
    val string = ProtocolMessageSerializer.serialize(clazz)
    val req = ProtocolMessageSerializer.deserialize[NewTransactionRequest](string)
    clazz shouldEqual req
  }
  "TStreams serializer" should "serialize and deserialize TransactionResponse" in {
    val clazz = TransactionResponse("snd", "rcv", UUID.randomUUID(), 228)
    val string = ProtocolMessageSerializer.serialize(clazz)
    val req = ProtocolMessageSerializer.deserialize[TransactionResponse](string)
    clazz shouldEqual req
  }
  "TStreams serializer" should "serialize and deserialize PTM" in {
    val clazz = TransactionStateMessage(UUID.randomUUID(), 123, TransactionStatus.postCheckpoint, 5, 1, 2)
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
    assert(ProtocolMessageSerializer.deserialize[TransactionStatus.ProducerTransactionStatus](ProtocolMessageSerializer.serialize(pre)) == pre)
    assert(ProtocolMessageSerializer.deserialize[TransactionStatus.ProducerTransactionStatus](ProtocolMessageSerializer.serialize(fin)) == fin)
    assert(ProtocolMessageSerializer.deserialize[TransactionStatus.ProducerTransactionStatus](ProtocolMessageSerializer.serialize(canceled)) == canceled)
    assert(ProtocolMessageSerializer.deserialize[TransactionStatus.ProducerTransactionStatus](ProtocolMessageSerializer.serialize(updated)) == updated)
    assert(ProtocolMessageSerializer.deserialize[TransactionStatus.ProducerTransactionStatus](ProtocolMessageSerializer.serialize(opened)) == opened)
  }
  "TStreams serializer" should "serialize and deserialize AgentSettings" in {
    val clazz = new AgentSettings("agent", 21212, 12121212)
    val string = ProtocolMessageSerializer.serialize(clazz)
    val req = ProtocolMessageSerializer.deserialize[AgentSettings](string)
    clazz shouldEqual req
  }
}
