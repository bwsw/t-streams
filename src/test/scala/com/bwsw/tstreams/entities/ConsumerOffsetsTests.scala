package com.bwsw.tstreams.entities

import com.bwsw.tstreams.testutils.{LocalGeneratorCreator, TestStorageServer, TestUtils}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}


class ConsumerOffsetsTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  val srv = TestStorageServer.get()
  val storageClient = f.getStorageClient()

  "ConsumerEntity.saveSingleOffset() ConsumerEntity.exist() ConsumerEntity.getOffset()" should "create new consumer with particular offset," +
    " then check consumer existence, then get this consumer offset" in {
    val consumer = getRandomString
    val stream = getRandomString
    val s = storageClient.createStream(stream, 1, 24 * 3600, "")
    val partition = 1
    val offset: Long = LocalGeneratorCreator.getTransaction()
    storageClient.saveConsumerOffset(consumer, s.id, partition, offset)

    Thread.sleep(500) // wait while server handle it.

    val checkExist: Boolean = storageClient.checkConsumerOffsetExists(consumer, s.id, partition)
    checkExist shouldBe true
    val retValOffset: Long = storageClient.getLastSavedConsumerOffset(consumer, s.id, partition)
    retValOffset shouldBe offset
  }

  "ConsumerEntity.exist()" should "return false if consumer not exist" in {
    val consumer = getRandomString
    val stream = getRandomString
    val s = storageClient.createStream(stream, 1, 24 * 3600, "test")
    val partition = 1
    storageClient.checkConsumerOffsetExists(consumer, s.id, partition) shouldEqual false
  }

  "ConsumerEntity.getOffset()" should "return -1 if consumer offset does not exist" in {
    val consumer = getRandomString
    val stream = getRandomString
    val s = storageClient.createStream(stream, 1, 24 * 3600, "")
    val partition = 1
    storageClient.getLastSavedConsumerOffset(consumer, s.id, partition) shouldBe -1
  }

  "ConsumerEntity.saveBatchOffset(); ConsumerEntity.getOffset()" should "create new consumer with particular offsets and " +
    "then validate this consumer offsets" in {
    val consumer = getRandomString
    val stream = getRandomString
    val s = storageClient.createStream(stream, 1, 24 * 3600, "")

    val offsets = scala.collection.mutable.Map[Int, Long]()
    for (i <- 0 to 100)
      offsets(i) = LocalGeneratorCreator.getTransaction()

    storageClient.saveConsumerOffsetBatch(consumer, s.id, offsets)

    Thread.sleep(500) // wait while server handle it.

    var checkVal = true

    for (i <- 0 to 100) {
      val id: Long = storageClient.getLastSavedConsumerOffset(consumer, s.id, i)
      checkVal &= id == offsets(i)
    }
    checkVal shouldBe true
  }

  override def afterAll(): Unit = {
    storageClient.shutdown()
    TestStorageServer.dispose(srv)
    onAfterAll()
  }
}
