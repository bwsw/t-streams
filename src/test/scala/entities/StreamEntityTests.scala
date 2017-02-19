package entities

import com.bwsw.tstreams.streams.Stream
import com.bwsw.tstreamstransactionserver.exception.Throwables.StreamNotExist
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{TestStorageServer, TestUtils}


class StreamEntityTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  val srv = TestStorageServer.get()
  val storageClient = f.getStorageClient()

  it should "check correctly dummy absent streams" in {
    storageClient.checkStreamExists("dummytrashy") shouldBe false
  }

  it should "create new stream" in {
    val name = getRandomString
    storageClient.createStream(name, 1, 24 * 3600, "sample-desc")

    val s: Stream = storageClient.loadStream(name)

    s.name shouldBe name
    s.description shouldBe "sample-desc"
    s.ttl shouldBe 24 * 3600
    s.partitionsCount shouldBe 1
  }

  it should "delete created stream" in {
    val name = getRandomString
    storageClient.createStream(name, 1, 24 * 3600, "sample-desc")
    storageClient.checkStreamExists(name) shouldBe true
    storageClient.deleteStream(name)
    storageClient.checkStreamExists(name) shouldBe false
  }

  "BasicStreamService.createStream()" should "create stream" in {
    val name = getRandomString

    val stream: Stream = storageClient.createStream(
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

      storageClient.createStream(
        streamName = name,
        partitionsCount = 3,
        ttl = 100,
        description = "some_description")

      storageClient.createStream(
        streamName = name,
        partitionsCount = 3,
        ttl = 100,
        description = "some_description")
    }
  }

  "BasicStreamService.loadStream()" should "load created stream" in {
    val name = getRandomString

    storageClient.createStream(
      streamName = name,
      partitionsCount = 3,
      ttl = 100,
      description = "some_description")

    val stream: Stream = storageClient.loadStream(name)
    val checkVal = stream.isInstanceOf[Stream]
    checkVal shouldBe true
  }

  "BasicStreamService.isExist()" should "say exist concrete stream or not" in {
    val name = getRandomString
    val dummyName = getRandomString

    storageClient.createStream(
      streamName = name,
      partitionsCount = 3,
      ttl = 100,
      description = "some_description")

    val isPresent = storageClient.checkStreamExists(name)
    isPresent shouldBe true
    val isAbsent = !storageClient.checkStreamExists(dummyName)
    isAbsent shouldBe true
  }

  "BasicStreamService.loadStream()" should "throw exception if stream not exist" in {
    val name = getRandomString

    intercept[StreamNotExist] {
      storageClient.loadStream(name)
    }
  }

  "BasicStreamService.deleteStream()" should "delete created stream" in {
    val name = getRandomString

    storageClient.createStream(
      streamName = name,
      partitionsCount = 3,
      ttl = 100,
      description = "some_description")

    storageClient.deleteStream(name)

    intercept[StreamNotExist] {
      storageClient.loadStream(name)
    }
  }

  "BasicStreamService.deleteStream()" should "throw exception if stream was not created before" in {
    val name = getRandomString
    storageClient.deleteStream(name)
  }

  override def afterAll(): Unit = {
    TestStorageServer.dispose(srv)
    onAfterAll()
  }
}
