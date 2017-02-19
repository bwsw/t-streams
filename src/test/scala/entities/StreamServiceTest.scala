package entities

import com.bwsw.tstreams.streams.StreamService
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{TestStorageServer, TestUtils}

/**
  * Created by ivan on 19.02.17.
  */
class StreamServiceTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  val srv = TestStorageServer.get()
  val storageClient = f.getStorageClient()

  it should "check correctly dummy absent streams" in {
    StreamService.checkExists(storageClient, "dummytrashy") shouldBe false
  }

  it should "create new stream" in {
    val name = getRandomString
    StreamService.createStream(storageClient, name, 1, 24 * 3600, "sample-desc")

    val s = StreamService.loadStream(storageClient, name)

    s.name shouldBe name
    s.description shouldBe "sample-desc"
    s.ttl shouldBe 24 * 3600
    s.partitionsCount shouldBe 1
  }

  it should "delete created stream" in {
    val name = getRandomString
    StreamService.createStream(storageClient, name, 1, 24 * 3600, "sample-desc")
    StreamService.checkExists(storageClient, name) shouldBe true
    StreamService.deleteStream(storageClient, name)
    StreamService.checkExists(storageClient, name) shouldBe false
  }

  override def afterAll(): Unit = {
    TestStorageServer.dispose(srv)
    onAfterAll()
  }
}
