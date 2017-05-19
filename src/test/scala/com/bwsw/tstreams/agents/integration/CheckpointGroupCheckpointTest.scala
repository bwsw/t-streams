package com.bwsw.tstreams.agents.integration

import java.util.concurrent.CountDownLatch

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.group.CheckpointGroup
import com.bwsw.tstreams.agents.producer.NewProducerTransactionPolicy
import com.bwsw.tstreams.testutils._
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}


class CheckpointGroupCheckpointTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  lazy val srv = TestStorageServer.get()

  lazy val producer = f.getProducer(
    name = "test_producer",
    partitions = Set(0, 1, 2))

  lazy val consumer = f.getConsumer(
    name = "test_consumer",
    partitions = Set(0, 1, 2),
    offset = Oldest,
    useLastOffset = true)

  lazy val consumer2 = f.getConsumer(
    name = "test_consumer",
    partitions = Set(0, 1, 2),
    offset = Oldest,
    useLastOffset = true)

  override def beforeAll(): Unit = {
    srv
    createNewStream()
    consumer.start
  }


  "Group commit" should "checkpoint all AgentsGroup state" in {
    val group = new CheckpointGroup()
    group.add(producer)
    group.add(consumer)

    val l1 = new CountDownLatch(1)
    val l2 = new CountDownLatch(1)

    val transaction1 = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)

    srv.notifyProducerTransactionCompleted(t =>
      transaction1.getTransactionID == t.transactionID && t.state == TransactionStates.Checkpointed, l1.countDown())

    logger.info("Transaction 1 is " + transaction1.getTransactionID.toString)
    transaction1.send("info1".getBytes())
    transaction1.checkpoint()

    l1.await()

    //move consumer offsets
    consumer.getTransaction(0).get

    //open transaction without close
    val transaction2 = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened, 1)

    srv.notifyProducerTransactionCompleted(t =>
      transaction2.getTransactionID == t.transactionID && t.state == TransactionStates.Checkpointed, l2.countDown())

    logger.info("Transaction 2 is " + transaction2.getTransactionID.toString)
    transaction2.send("info2".getBytes())

    group.checkpoint()

    l2.await()

    consumer2.start()
    //assert that the second transaction was closed and consumer offsets was moved
    consumer2.getTransaction(1).get.getAll.head shouldBe "info2".getBytes()
  }

  override def afterAll(): Unit = {
    producer.stop()
    Seq(consumer, consumer2).foreach(c => c.stop())
    TestStorageServer.dispose(srv)
    onAfterAll()
  }
}
