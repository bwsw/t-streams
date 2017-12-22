package it

import com.bwsw.tstreamstransactionserver.netty.server.db.zk.ZookeeperStreamRepository
import com.bwsw.tstreamstransactionserver.netty.server.streamService.{StreamKey, StreamRecord, StreamValue}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import util.Utils._

class StreamRecordValueDatabaseZkTest
  extends FlatSpec
    with Matchers
    with BeforeAndAfterEach
{
  private val path = "/tts/test_path"
  private def getStreamValue = StreamValue(
    "test_stream",
    20,
    None,
    5,
    Some(s"$path/ids/id0000000000")
  )


  "One" should "put stream and get it back" in {
    val (zkServer, zkClient) = startZkServerAndGetIt

    val zkDatabase = new ZookeeperStreamRepository(zkClient, path)

    val streamValue  = getStreamValue

    val streamKey    = zkDatabase.put(streamValue)
    val streamRecordByName = zkDatabase.get(streamValue.name)
    val streamRecordByID = zkDatabase.get(streamKey)

    streamRecordByID shouldBe defined
    val streamObj = streamRecordByID.get

    streamObj.key shouldBe streamKey
    streamObj.stream shouldBe streamValue
    streamObj.stream shouldBe streamRecordByName.get.stream

    zkClient.close()
    zkServer.close()
  }

  it should "put stream, try to put new stream with the same and got exception" in {
    val (zkServer, zkClient) = startZkServerAndGetIt

    val zkDatabase = new ZookeeperStreamRepository(zkClient, path)

    val streamValue = getStreamValue
    zkDatabase.put(streamValue)

    val newStream = StreamValue("test_stream", 100, Some("overwrite"), 10, None)


    zkDatabase.put(newStream) shouldBe StreamKey(-1)

    zkDatabase.exists(newStream.name) shouldBe true

    zkClient.close()
    zkServer.close()
  }

  it should "put stream, delete it, then the one calls getStream and it returns None" in {
    val (zkServer, zkClient) = startZkServerAndGetIt

    val zkDatabase = new ZookeeperStreamRepository(zkClient, path)

    val streamValue  = getStreamValue

    zkDatabase.put(streamValue)
    zkDatabase.delete(streamValue.name) shouldBe true
    val streamRecord = zkDatabase.get(streamValue.name)

    streamRecord should not be defined

    zkClient.close()
    zkServer.close()
  }

  it should "put stream, delete it, then on checking stream the one see stream doesn't exist" in {
    val (zkServer, zkClient) = startZkServerAndGetIt

    val zkDatabase = new ZookeeperStreamRepository(zkClient, path)

    val streamValue  = getStreamValue

    zkDatabase.put(streamValue)
    zkDatabase.delete(streamValue.name) shouldBe true
    val streamRecord = zkDatabase.exists(streamValue.name)

    streamRecord shouldBe false

    zkClient.close()
    zkServer.close()
  }

  it should "put stream, delete it, then put a new stream with same name a get it back" in {
    val (zkServer, zkClient) = startZkServerAndGetIt

    val zkDatabase = new ZookeeperStreamRepository(zkClient, path)

    val streamValue = getStreamValue
    zkDatabase.put(streamValue)
    zkDatabase.delete(streamValue.name) shouldBe true

    val newStream = StreamValue(
      "test_stream",
      100,
      Some("overwrite"),
      10,
      Some(s"$path/ids/id0000000001")
    )
    zkDatabase.put(newStream)

    zkDatabase.exists(newStream.name) shouldBe true

    val streamRecord = zkDatabase.get(newStream.name)
    streamRecord shouldBe defined
    streamRecord.get.stream shouldBe newStream

    zkClient.close()
    zkServer.close()
  }

  it should "put stream, delete it, then get it by ID and see there the stream record" in {
    val (zkServer, zkClient) = startZkServerAndGetIt

    val zkDatabase = new ZookeeperStreamRepository(zkClient, path)

    val streamValue = getStreamValue
    val streamKey = zkDatabase.put(streamValue)

    zkDatabase.delete(streamValue.name) shouldBe true
    zkDatabase.exists(streamValue.name) shouldBe false

    val retrievedStream = zkDatabase.get(streamKey)

    retrievedStream shouldBe defined

    retrievedStream.get shouldBe StreamRecord(streamKey, streamValue)

    zkClient.close()
    zkServer.close()
  }

  it should "put stream, delete it, then get it by ID and see there the stream record even if stream is overwritten" in {
    val (zkServer, zkClient) = startZkServerAndGetIt

    val zkDatabase = new ZookeeperStreamRepository(zkClient, path)

    val streamValue = getStreamValue
    val streamKey = zkDatabase.put(streamValue)

    zkDatabase.delete(streamValue.name) shouldBe true
    zkDatabase.exists(streamValue.name) shouldBe false

    val newStream = StreamValue("test_stream", 100, Some("overwrite"), 10, None)
    zkDatabase.put(newStream)

    val retrievedStream = zkDatabase.get(streamKey)

    retrievedStream shouldBe defined

    retrievedStream.get shouldBe StreamRecord(streamKey, streamValue)

    zkClient.close()
    zkServer.close()
  }

}
