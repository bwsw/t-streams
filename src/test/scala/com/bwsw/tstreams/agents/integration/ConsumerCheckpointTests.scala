package com.bwsw.tstreams.agents.integration

import java.util.concurrent.CountDownLatch

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.producer.NewProducerTransactionPolicy
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 09.09.16.
  */
class ConsumerCheckpointTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  lazy val srv = TestStorageServer.getNewClean()
  override def beforeAll(): Unit = {
    srv
    createNewStream()
  }


  it should "handle checkpoints correctly" in {

    val CONSUMER_NAME = "test_consumer"

    val producer = f.getProducer(
      name = "test_producer",
      partitions = Set(0))

    val c1 = f.getConsumer(
      name = CONSUMER_NAME,
      partitions = Set(0),
      offset = Oldest,
      useLastOffset = false)

    val c2 = f.getConsumer(
      name = CONSUMER_NAME,
      partitions = Set(0),
      offset = Oldest,
      useLastOffset = true)

    val t1 = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened, 0)
    t1.send("data".getBytes())
    producer.checkpoint()

    val t2 = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened, 0)
    val l2 = new CountDownLatch(1)
    srv.notifyProducerTransactionCompleted(t => t.transactionID == t2.getTransactionID && t.state == TransactionStates.Checkpointed, l2.countDown())
    t2.send("data".getBytes())
    producer.checkpoint()
    l2.await()

    c1.start()
    c1.getTransactionById(0, t1.getTransactionID).isDefined shouldBe true
    c1.getTransactionById(0, t2.getTransactionID).isDefined shouldBe true

    val l = new CountDownLatch(1)
    srv.notifyConsumerTransactionCompleted(ct => t1.getTransactionID == ct.transactionID, l.countDown())

    c1.getTransaction(0).get.getTransactionID shouldBe t1.getTransactionID
    c1.checkpoint()
    c1.stop()


    l.await()

    c2.start()
    c2.getTransaction(0).get.getTransactionID shouldBe t2.getTransactionID
    c2.checkpoint()
    c2.getTransaction(0).isDefined shouldBe false
    c2.stop()

    producer.stop()
  }

  override def afterAll(): Unit = {
    TestStorageServer.dispose(srv)
    onAfterAll()
  }
}
