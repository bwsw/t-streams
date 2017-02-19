package services

import com.bwsw.tstreams.common.StorageClient
import com.bwsw.tstreams.streams.{Stream, StreamService}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{TestStorageServer, TestUtils}


class TStreamServiceTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  val storageClient: StorageClient = null
  val storageServer = TestStorageServer.get()

  "BasicStreamService.createStream()" should "create stream" in {
    val name = getRandomString

    val stream: Stream = StreamService.createStream(
      storageClient = storageClient,
      streamName = name,
      partitionsCount = 3,
      ttl = 100,
      description = "some_description")

    val checkVal = stream.isInstanceOf[Stream]

    checkVal shouldBe true
  }

  "BasicStreamService.createStream()" should "throw exception if stream already created" in {
    intercept[IllegalArgumentException] {
      val name = getRandomString

      StreamService.createStream(
        storageClient = storageClient,
        streamName = name,
        partitionsCount = 3,
        ttl = 100,
        description = "some_description")

      StreamService.createStream(
        storageClient = storageClient,
        streamName = name,
        partitionsCount = 3,
        ttl = 100,
        description = "some_description")
    }
  }

  "BasicStreamService.loadStream()" should "load created stream" in {
    val name = getRandomString

    StreamService.createStream(
      storageClient = storageClient,
      streamName = name,
      partitionsCount = 3,
      ttl = 100,
      description = "some_description")

    val stream: Stream = StreamService.loadStream(storageClient = storageClient, name)
    val checkVal = stream.isInstanceOf[Stream]
    checkVal shouldBe true
  }

  "BasicStreamService.isExist()" should "say exist concrete stream or not" in {
    val name = getRandomString
    val dummyName = getRandomString

    StreamService.createStream(
      storageClient = storageClient,
      streamName = name,
      partitionsCount = 3,
      ttl = 100,
      description = "some_description")

    val isPresent = StreamService.checkExists(storageClient = storageClient, name)
    isPresent shouldBe true
    val isAbsent = !StreamService.checkExists(storageClient = storageClient, dummyName)
    isAbsent shouldBe true
  }

  "BasicStreamService.loadStream()" should "throw exception if stream not exist" in {
    val name = getRandomString

    intercept[IllegalArgumentException] {
      StreamService.loadStream(storageClient = storageClient,name)
    }
  }

  "BasicStreamService.deleteStream()" should "delete created stream" in {
    val name = getRandomString

    StreamService.createStream(
      storageClient = storageClient,
      streamName = name,
      partitionsCount = 3,
      ttl = 100,
      description = "some_description")

    StreamService.deleteStream(storageClient = storageClient,name)

    intercept[IllegalArgumentException] {
      StreamService.loadStream(storageClient = storageClient, name)
    }
  }

  "BasicStreamService.deleteStream()" should "throw exception if stream was not created before" in {
    val name = getRandomString

    intercept[IllegalArgumentException] {
      StreamService.deleteStream(storageClient = storageClient, name)
    }
  }

  override def afterAll(): Unit = {
    TestStorageServer.dispose(storageServer)
    onAfterAll()
  }
}
