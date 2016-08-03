package common

import java.util.UUID
import com.bwsw.tstreams.common.TStreamsProtocolMessageSerializer$
import com.bwsw.tstreams.coordination.pubsub.messages.{ProducerTopicMessage, ProducerTransactionStatus}
import com.bwsw.tstreams.coordination.transactions.messages._
import com.bwsw.tstreams.coordination.transactions.peertopeer.AgentSettings
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.TestUtils


class ProtocolMessageSerializerTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  val serializer = new TStreamsProtocolMessageSerializer

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
    val clazz = PublishRequest("snd", "rcv", ProducerTopicMessage(UUID.randomUUID(), 228, ProducerTransactionStatus.cancel, 1488))
    val string = serializer.serialize(clazz)
    val req = serializer.deserialize[PublishRequest](string)
    clazz shouldEqual req
  }
  "TStreams serializer" should "serialize and deserialize PublishResponse" in {
    val clazz = PublishResponse("snd", "rcv", ProducerTopicMessage(UUID.randomUUID(), 228, ProducerTransactionStatus.cancel, 1488))
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
    val clazz = ProducerTopicMessage(UUID.randomUUID(), 123, ProducerTransactionStatus.postCheckpoint, 5)
    val string = serializer.serialize(clazz)
    val req = serializer.deserialize[ProducerTopicMessage](string)
    clazz shouldEqual req
  }
  "TStreams serializer" should "serialize and deserialize PTS" in {
    val pre = ProducerTransactionStatus.preCheckpoint
    val fin = ProducerTransactionStatus.postCheckpoint
    val canceled = ProducerTransactionStatus.cancel
    val updated = ProducerTransactionStatus.update
    val opened = ProducerTransactionStatus.opened
    assert(serializer.deserialize[ProducerTransactionStatus.ProducerTransactionStatus](serializer.serialize(pre)) == pre)
    assert(serializer.deserialize[ProducerTransactionStatus.ProducerTransactionStatus](serializer.serialize(fin)) == fin)
    assert(serializer.deserialize[ProducerTransactionStatus.ProducerTransactionStatus](serializer.serialize(canceled)) == canceled)
    assert(serializer.deserialize[ProducerTransactionStatus.ProducerTransactionStatus](serializer.serialize(updated)) == updated)
    assert(serializer.deserialize[ProducerTransactionStatus.ProducerTransactionStatus](serializer.serialize(opened)) == opened)
  }
  "TStreams serializer" should "serialize and deserialize AgentSettings" in {
    val clazz = new AgentSettings("agent", 21212, 12121212)
    val string = serializer.serialize(clazz)
    val req = serializer.deserialize[AgentSettings](string)
    clazz shouldEqual req
  }
}
