package services

import java.net.InetSocketAddress

import com.bwsw.tstreams.services.BasicStreamService
import com.bwsw.tstreams.streams.BasicStream
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.TestUtils


class BasicStreamServiceTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  val storageInst = storageFactory.getInstance(aerospikeOptions)
  val metadataStorageInst = metadataStorageFactory.getInstance(
    cassandraHosts = List(new InetSocketAddress("localhost", 9042)),
    keyspace = randomKeyspace)


  "BasicStreamService.createStream()" should "create stream" in {
    val name = randomString

    val stream: BasicStream[_] = BasicStreamService.createStream(
      streamName = name,
      partitions = 3,
      ttl = 100,
      description = "some_description",
      metadataStorage = metadataStorageInst,
      dataStorage = storageInst)

    val checkVal = stream.isInstanceOf[BasicStream[_]]

    checkVal shouldBe true
  }

  "BasicStreamService.createStream()" should "throw exception if stream already created" in {
    intercept[IllegalArgumentException] {
      val name = randomString

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
    val name = randomString

    BasicStreamService.createStream(
      streamName = name,
      partitions = 3,
      ttl = 100,
      description = "some_description",
      metadataStorage = metadataStorageInst,
      dataStorage = storageInst)

    val stream: BasicStream[_] = BasicStreamService.loadStream(name, metadataStorageInst, storageInst)
    val checkVal = stream.isInstanceOf[BasicStream[_]]
    checkVal shouldBe true
  }

  "BasicStreamService.isExist()" should "say exist concrete stream or not" in {
    val name = randomString
    val notExistName = randomString

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
    val name = randomString

    intercept[IllegalArgumentException] {
      BasicStreamService.loadStream(name, metadataStorageInst, storageInst)
    }
  }

  "BasicStreamService.deleteStream()" should "delete created stream" in {
    val name = randomString

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
    val name = randomString

    intercept[IllegalArgumentException] {
      BasicStreamService.deleteStream(name, metadataStorageInst)
    }
  }

  override def afterAll(): Unit = {
    onAfterAll()
  }
}
