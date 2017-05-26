package com.bwsw.tstreams.agents.integration

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.consumer.Offset.Newest
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.agents.producer.NewProducerTransactionPolicy
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Created by ivan on 13.09.16.
  */
class CheckpointGroupAndSubscriberEventsTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  lazy val srv = TestStorageServer.getNewClean()

  override def beforeAll(): Unit = {
    srv
    createNewStream()
  }

  override def afterAll(): Unit = {
    TestStorageServer.dispose(srv)
    onAfterAll()
  }

  it should "checkpoint all transactions with CG" in {
    val l = new CountDownLatch(1)
    var transactionsCounter: Int = 0

    val group = f.getCheckpointGroup()

    val producer = f.getProducer(
      name = "test_producer",
      partitions = Set(0))

    group.add(producer)

    val subscriber = f.getSubscriber(name = "ss+2",
      partitions = Set(0),
      offset = Newest,
      useLastOffset = true,
      callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => this.synchronized {
        transactionsCounter += 1
        val data = new String(transaction.getAll.head)
        data shouldBe "test"
        if (transactionsCounter == 2) {
          l.countDown()
        }
      })
    subscriber.start()
    val txn1 = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened, 0)
    txn1.send("test".getBytes())
    group.checkpoint()
    val txn2 = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened, 0)
    txn2.send("test".getBytes())
    group.checkpoint()
    l.await(5, TimeUnit.SECONDS) shouldBe true
    transactionsCounter shouldBe 2
    subscriber.stop()
    producer.stop()
  }

}