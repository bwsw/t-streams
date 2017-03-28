package entities

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{LocalGeneratorCreator, TestStorageServer, TestUtils}


class ConsumerOffsetsTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  val srv = TestStorageServer.get()
  val storageClient = f.getStorageClient()

  "ConsumerEntity.saveSingleOffset() ConsumerEntity.exist() ConsumerEntity.getOffset()" should "create new consumer with particular offset," +
    " then check consumer existence, then get this consumer offset" in {
    val consumer = getRandomString
    val stream = getRandomString
    storageClient.createStream(stream, 1, 24 * 3600, "")
    val partition = 1
    val offset: Long = LocalGeneratorCreator.getTransaction()
    storageClient.saveConsumerOffset(consumer, stream, partition, offset)

    Thread.sleep(500) // wait while server handle it.

    val checkExist: Boolean = storageClient.checkConsumerOffsetExists(consumer, stream, partition)
    checkExist shouldBe true
    val retValOffset: Long = storageClient.getLastSavedConsumerOffset(consumer, stream, partition)
    retValOffset shouldBe offset
  }

  "ConsumerEntity.exist()" should "return false if consumer not exist" in {
    val consumer = getRandomString
    val stream = getRandomString
    storageClient.createStream(stream, 1, 24 * 3600, "")
    val partition = 1
    storageClient.checkConsumerOffsetExists(consumer, stream, partition) shouldEqual false
  }

  "ConsumerEntity.getOffset()" should "return -1 if consumer offset does not exist" in {
    val consumer = getRandomString
    val stream = getRandomString
    storageClient.createStream(stream, 1, 24 * 3600, "")
    val partition = 1
    storageClient.getLastSavedConsumerOffset(consumer, stream, partition) shouldBe -1
  }

  "ConsumerEntity.saveBatchOffset(); ConsumerEntity.getOffset()" should "create new consumer with particular offsets and " +
    "then validate this consumer offsets" in {
    val consumer = getRandomString
    val stream = getRandomString
    storageClient.createStream(stream, 1, 24 * 3600, "")

    val offsets = scala.collection.mutable.Map[Int, Long]()
    for (i <- 0 to 100)
      offsets(i) = LocalGeneratorCreator.getTransaction()

    storageClient.saveConsumerOffsetBatch(consumer, stream, offsets)

    Thread.sleep(500) // wait while server handle it.

    var checkVal = true

    for (i <- 0 to 100) {
      val id: Long = storageClient.getLastSavedConsumerOffset(consumer, stream, i)
      checkVal &= id == offsets(i)
    }
    checkVal shouldBe true
  }

  override def afterAll(): Unit = {
    TestStorageServer.dispose(srv)
    onAfterAll()
  }
}
