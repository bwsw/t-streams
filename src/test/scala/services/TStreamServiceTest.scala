package services

import java.net.InetSocketAddress

import com.bwsw.tstreams.streams.{Stream, StreamService}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{RandomStringCreator, TestUtils}


class TStreamServiceTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  val storageInst = hazelcastStorageFactory.getInstance(hazelcastOptions)
  val metadataStorageInst = metadataStorageFactory.getInstance(
    CassandraConnectorConf(Set(new InetSocketAddress("localhost", 9142))),
    keyspace = randomKeyspace)

  def randomVal: String = RandomStringCreator.randomAlphaString(10)


  "BasicStreamService.createStream()" should "create stream" in {
    val name = randomVal

    val stream: Stream[_] = StreamService.createStream(
      streamName = name,
      partitions = 3,
      ttl = 100,
      description = "some_description",
      metadataStorage = metadataStorageInst,
      dataStorage = storageInst)

    val checkVal = stream.isInstanceOf[Stream[_]]

    checkVal shouldBe true
  }

  "BasicStreamService.createStream()" should "throw exception if stream already created" in {
    intercept[IllegalArgumentException] {
      val name = randomVal

      StreamService.createStream(
        streamName = name,
        partitions = 3,
        ttl = 100,
        description = "some_description",
        metadataStorage = metadataStorageInst,
        dataStorage = storageInst)

      StreamService.createStream(
        streamName = name,
        partitions = 3,
        ttl = 100,
        description = "some_description",
        metadataStorage = metadataStorageInst,
        dataStorage = storageInst)
    }
  }

  "BasicStreamService.loadStream()" should "load created stream" in {
    val name = randomVal

    StreamService.createStream(
      streamName = name,
      partitions = 3,
      ttl = 100,
      description = "some_description",
      metadataStorage = metadataStorageInst,
      dataStorage = storageInst)

    val stream: Stream[_] = StreamService.loadStream(name, metadataStorageInst, storageInst)
    val checkVal = stream.isInstanceOf[Stream[_]]
    checkVal shouldBe true
  }

  "BasicStreamService.isExist()" should "say exist concrete stream or not" in {
    val name = randomVal
    val notExistName = randomVal

    StreamService.createStream(
      streamName = name,
      partitions = 3,
      ttl = 100,
      description = "some_description",
      metadataStorage = metadataStorageInst,
      dataStorage = storageInst)

    val isExist = StreamService.doesExist(name, metadataStorageInst)
    val isNotExist = StreamService.doesExist(notExistName, metadataStorageInst)
    val checkVal = isExist && !isNotExist

    checkVal shouldBe true
  }

  "BasicStreamService.loadStream()" should "throw exception if stream not exist" in {
    val name = randomVal

    intercept[IllegalArgumentException] {
      StreamService.loadStream(name, metadataStorageInst, storageInst)
    }
  }

  "BasicStreamService.deleteStream()" should "delete created stream" in {
    val name = randomVal

    StreamService.createStream(
      streamName = name,
      partitions = 3,
      ttl = 100,
      description = "some_description",
      metadataStorage = metadataStorageInst,
      dataStorage = storageInst)

    StreamService.deleteStream(name, metadataStorageInst)

    intercept[IllegalArgumentException] {
      StreamService.loadStream(name, metadataStorageInst, storageInst)
    }
  }

  "BasicStreamService.deleteStream()" should "throw exception if stream was not created before" in {
    val name = randomVal

    intercept[IllegalArgumentException] {
      StreamService.deleteStream(name, metadataStorageInst)
    }
  }

  override def afterAll(): Unit = {
    onAfterAll()
  }
}
