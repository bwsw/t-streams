package common

import java.util.UUID

import com.bwsw.tstreams.common.ProtocolMessageSerializer
import com.bwsw.tstreams.coordination.messages.master._
import com.bwsw.tstreams.coordination.messages.state.{Message, TransactionStatus}
import com.bwsw.tstreams.coordination.producer.p2p.AgentSettings
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.TestUtils


class ProtocolMessageSerializerTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  val serializer = new ProtocolMessageSerializer

  "TStreams serializer" should "serialize and deserialize DeleteMasterRequest" in {
    val clazz = DeleteMasterRequest("snd", "rcv", 0)
    val string = serializer.serialize(clazz)
    val req = serializer.deserialize[DeleteMasterRequest](string)
    clazz shouldEqual req
  }
  "TStreams serializer" should "serialize and deserialize DeleteMasterResponse" in {
    val clazz = DeleteMasterResponse("snd", "rcv", 0)
    val string = serializer.serialize(clazz)
    val req = serializer.deserialize[DeleteMasterResponse](string)
    clazz shouldEqual req
  }
  "TStreams serializer" should "serialize and deserialize EmptyRequest" in {
    val clazz = EmptyRequest("snd", "rcv", 0)
    val string = serializer.serialize(clazz)
    val req = serializer.deserialize[EmptyRequest](string)
    clazz shouldEqual req
  }
  "TStreams serializer" should "serialize and deserialize EmptyResponse" in {
    val clazz = EmptyResponse("snd", "rcv", 0)
    val string = serializer.serialize(clazz)
    val req = serializer.deserialize[EmptyResponse](string)
    clazz shouldEqual req
  }
  "TStreams serializer" should "serialize and deserialize PingRequest" in {
    val clazz = PingRequest("snd", "rcv", 0)
    val string = serializer.serialize(clazz)
    val req = serializer.deserialize[PingRequest](string)
    clazz shouldEqual req
  }
  "TStreams serializer" should "serialize and deserialize PingResponse" in {
    val clazz = PingResponse("snd", "rcv", 0)
    val string = serializer.serialize(clazz)
    val req = serializer.deserialize[PingResponse](string)
    clazz shouldEqual req
  }
  "TStreams serializer" should "serialize and deserialize PublishRequest" in {
    val clazz = PublishRequest("snd", "rcv", Message(UUID.randomUUID(), 228, TransactionStatus.cancel, 1488))
    val string = serializer.serialize(clazz)
    val req = serializer.deserialize[PublishRequest](string)
    clazz shouldEqual req
  }
  "TStreams serializer" should "serialize and deserialize PublishResponse" in {
    val clazz = PublishResponse("snd", "rcv", Message(UUID.randomUUID(), 228, TransactionStatus.cancel, 1488))
    val string = serializer.serialize(clazz)
    val req = serializer.deserialize[PublishResponse](string)
    clazz shouldEqual req
  }
  "TStreams serializer" should "serialize and deserialize SetMasterRequest" in {
    val clazz = SetMasterRequest("snd", "rcv", -1)
    val string = serializer.serialize(clazz)
    val req = serializer.deserialize[SetMasterRequest](string)
    clazz shouldEqual req
  }
  "TStreams serializer" should "serialize and deserialize SetMasterResponse" in {
    val clazz = SetMasterResponse("snd", "rcv", -1)
    val string = serializer.serialize(clazz)
    val req = serializer.deserialize[SetMasterResponse](string)
    clazz shouldEqual req
  }
  "TStreams serializer" should "serialize and deserialize TransactionRequest" in {
    val clazz = TransactionRequest("snd", "rcv", -1)
    val string = serializer.serialize(clazz)
    val req = serializer.deserialize[TransactionRequest](string)
    clazz shouldEqual req
  }
  "TStreams serializer" should "serialize and deserialize TransactionResponse" in {
    val clazz = TransactionResponse("snd", "rcv", UUID.randomUUID(), 228)
    val string = serializer.serialize(clazz)
    val req = serializer.deserialize[TransactionResponse](string)
    clazz shouldEqual req
  }
  "TStreams serializer" should "serialize and deserialize PTM" in {
    val clazz = Message(UUID.randomUUID(), 123, TransactionStatus.postCheckpoint, 5)
    val string = serializer.serialize(clazz)
    val req = serializer.deserialize[Message](string)
    clazz shouldEqual req
  }
  "TStreams serializer" should "serialize and deserialize PTS" in {
    val pre = TransactionStatus.preCheckpoint
    val fin = TransactionStatus.postCheckpoint
    val canceled = TransactionStatus.cancel
    val updated = TransactionStatus.update
    val opened = TransactionStatus.opened
    assert(serializer.deserialize[TransactionStatus.ProducerTransactionStatus](serializer.serialize(pre)) == pre)
    assert(serializer.deserialize[TransactionStatus.ProducerTransactionStatus](serializer.serialize(fin)) == fin)
    assert(serializer.deserialize[TransactionStatus.ProducerTransactionStatus](serializer.serialize(canceled)) == canceled)
    assert(serializer.deserialize[TransactionStatus.ProducerTransactionStatus](serializer.serialize(updated)) == updated)
    assert(serializer.deserialize[TransactionStatus.ProducerTransactionStatus](serializer.serialize(opened)) == opened)
  }
  "TStreams serializer" should "serialize and deserialize AgentSettings" in {
    val clazz = new AgentSettings("agent", 21212, 12121212)
    val string = serializer.serialize(clazz)
    val req = serializer.deserialize[AgentSettings](string)
    clazz shouldEqual req
  }
}
