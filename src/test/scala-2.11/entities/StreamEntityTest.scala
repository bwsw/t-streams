package entities

import com.bwsw.tstreams.entities.StreamEntity
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{RandomStringCreator, TestUtils}


class StreamEntityTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  def randomVal: String = RandomStringCreator.randomAlphaString(10)

  val connectedSession = cluster.connect(randomKeyspace)

  "StreamEntity.createStream() StreamEntity.getStream()" should "create and retrieve created stream from metadata tables" in {
    val streamEntity = new StreamEntity("streams", connectedSession)
    val streamName = randomVal
    val partitions = 3
    val ttl = 3
    val description = randomVal
    streamEntity.createStream(streamName, partitions, ttl, description)
    val streamSettings = streamEntity.getStream(streamName).get

    assert(streamSettings.name == streamName
      && streamSettings.partitions == partitions
      && streamSettings.description == description
      && streamSettings.ttl == ttl)
  }

  "StreamEntity.createStream() StreamEntity.alternate() StreamEntity.getStream()" should
    "create, alternate and retrieve created stream from metadata tables" in {
    val streamEntity = new StreamEntity("streams", connectedSession)
    val streamName = randomVal
    val partitions = 3
    val ttl = 3
    val description = randomVal
    streamEntity.createStream(streamName, partitions, ttl, description)
    streamEntity.alternateStream(streamName, partitions + 1, ttl + 1, description + "a")
    val streamSettings = streamEntity.getStream(streamName).get

    assert(streamSettings.name == streamName
      && streamSettings.partitions == partitions + 1
      && streamSettings.description == description + "a"
      && streamSettings.ttl == ttl + 1)
  }

  "StreamEntity.createStream() StreamEntity.delete() StreamEntity.isExist()" should
    "create, delete and checking existence of stream from metadata tables" in {
    val streamEntity = new StreamEntity("streams", connectedSession)
    val streamName = randomVal
    val partitions = 3
    val ttl = 3
    val description = randomVal

    streamEntity.createStream(streamName, partitions, ttl, description)
    streamEntity.deleteStream(streamName)
    streamEntity.isExist(streamName) shouldEqual false
  }


  "StreamEntity.createStream() StreamEntity.createStream()" should
    "throw exception because two streams with equivalent name can't exist" in {
    val streamEntity = new StreamEntity("streams", connectedSession)
    val streamName = randomVal
    val partitions = 3
    val ttl = 3
    val description = randomVal
    streamEntity.createStream(streamName, partitions, ttl, description)

    intercept[IllegalArgumentException] {
      streamEntity.createStream(streamName, partitions, ttl, description)
    }
  }

  "StreamEntity.alternateStream()" should
    "throw exception on non existing stream" in {
    val streamEntity = new StreamEntity("streams", connectedSession)
    val streamName = randomVal
    val partitions = 3
    val ttl = 3
    val description = randomVal

    intercept[IllegalArgumentException] {
      streamEntity.alternateStream(streamName, partitions, ttl, description)
    }
  }

  override def afterAll(): Unit = {
    connectedSession.close()
    onAfterAll()
  }
}
