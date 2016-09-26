package services

import java.net.InetSocketAddress

import com.bwsw.tstreams.common.CassandraConnectorConf
import com.bwsw.tstreams.services.BasicStreamService
import com.bwsw.tstreams.streams.Stream
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

    val stream: Stream[_] = BasicStreamService.createStream(
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

      BasicStreamService.createStream(
        streamName = name,
        partitions = 3,
        ttl = 100,
        description = "some_description",
        metadataStorage = metadataStorageInst,
        dataStorage = storageInst)

      BasicStreamService.createStream(
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

    BasicStreamService.createStream(
      streamName = name,
      partitions = 3,
      ttl = 100,
      description = "some_description",
      metadataStorage = metadataStorageInst,
      dataStorage = storageInst)

    val stream: Stream[_] = BasicStreamService.loadStream(name, metadataStorageInst, storageInst)
    val checkVal = stream.isInstanceOf[Stream[_]]
    checkVal shouldBe true
  }

  "BasicStreamService.isExist()" should "say exist concrete stream or not" in {
    val name = randomVal
    val notExistName = randomVal

    BasicStreamService.createStream(
      streamName = name,
      partitions = 3,
      ttl = 100,
      description = "some_description",
      metadataStorage = metadataStorageInst,
      dataStorage = storageInst)

    val isExist = BasicStreamService.isExist(name, metadataStorageInst)
    val isNotExist = BasicStreamService.isExist(notExistName, metadataStorageInst)
    val checkVal = isExist && !isNotExist

    checkVal shouldBe true
  }

  "BasicStreamService.loadStream()" should "throw exception if stream not exist" in {
    val name = randomVal

    intercept[IllegalArgumentException] {
      BasicStreamService.loadStream(name, metadataStorageInst, storageInst)
    }
  }

  "BasicStreamService.deleteStream()" should "delete created stream" in {
    val name = randomVal

    BasicStreamService.createStream(
      streamName = name,
      partitions = 3,
      ttl = 100,
      description = "some_description",
      metadataStorage = metadataStorageInst,
      dataStorage = storageInst)

    BasicStreamService.deleteStream(name, metadataStorageInst)

    intercept[IllegalArgumentException] {
      BasicStreamService.loadStream(name, metadataStorageInst, storageInst)
    }
  }

  "BasicStreamService.deleteStream()" should "throw exception if stream was not created before" in {
    val name = randomVal

    intercept[IllegalArgumentException] {
      BasicStreamService.deleteStream(name, metadataStorageInst)
    }
  }

  override def afterAll(): Unit = {
    onAfterAll()
  }
}
