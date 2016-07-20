package common

import java.util.UUID

import com.bwsw.tstreams.common.serializer.TStreamsSerializer
import com.bwsw.tstreams.coordination.pubsub.messages.{ProducerTopicMessage, ProducerTransactionStatus}
import com.bwsw.tstreams.coordination.transactions.messages._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.TestUtils


class TStreamsSerializerTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils{
  val serializer = new TStreamsSerializer


//  serializer.serialize(PublishRequest)
//  serializer.serialize(PublishResponse)
//  serializer.serialize(SetMasterRequest)
//  serializer.serialize(SetMasterResponse)
//  serializer.serialize(TransactionRequest)
//  serializer.serialize(TransactionResponse)
//
//  serializer.serialize(ProducerTopicMessage)
//  serializer.serialize(ProducerTransactionStatus.opened)
//  serializer.serialize(ProducerTransactionStatus.cancelled)
//  serializer.serialize(ProducerTransactionStatus.preCheckpoint)
//  serializer.serialize(ProducerTransactionStatus.updated)
//  serializer.serialize(ProducerTransactionStatus.finalCheckpoint)

  "TStreams serializer" should "serialize and deserialize DeleteMasterRequest" in {
    val clazz = DeleteMasterRequest("snd","rcv",0)
    val string = serializer.serialize(clazz)
    val req = serializer.deserialize[DeleteMasterRequest](string)
    clazz shouldEqual req
  }
  "TStreams serializer" should "serialize and deserialize DeleteMasterResponse" in {
    val clazz = DeleteMasterResponse("snd","rcv",0)
    val string = serializer.serialize(clazz)
    val req = serializer.deserialize[DeleteMasterResponse](string)
    clazz shouldEqual req
  }
  "TStreams serializer" should "serialize and deserialize EmptyRequest" in {
    val clazz = EmptyRequest("snd","rcv",0)
    val string = serializer.serialize(clazz)
    val req = serializer.deserialize[EmptyRequest](string)
    clazz shouldEqual req
  }
  "TStreams serializer" should "serialize and deserialize EmptyResponse" in {
    val clazz = EmptyResponse("snd","rcv",0)
    val string = serializer.serialize(clazz)
    val req = serializer.deserialize[EmptyResponse](string)
    clazz shouldEqual req
  }
  "TStreams serializer" should "serialize and deserialize PingRequest" in {
    val clazz = PingRequest("snd","rcv",0)
    val string = serializer.serialize(clazz)
    val req = serializer.deserialize[PingRequest](string)
    clazz shouldEqual req
  }
  "TStreams serializer" should "serialize and deserialize PingResponse" in {
    val clazz = PingResponse("snd","rcv",0)
    val string = serializer.serialize(clazz)
    val req = serializer.deserialize[PingResponse](string)
    clazz shouldEqual req
  }
  "TStreams serializer" should "serialize and deserialize PublishRequest" in {
    val clazz = PublishRequest("snd","rcv",ProducerTopicMessage(UUID.randomUUID(),228,ProducerTransactionStatus.cancelled,1488))
    val string = serializer.serialize(clazz)
    println(string)
//    val req = serializer.deserialize[PublishRequest](string)
//    clazz shouldEqual req
  }
}
