package com.bwsw.tstreams.agents.integration

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.consumer.Offset.Newest
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.agents.producer.NewProducerTransactionPolicy
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 14.09.16.
  */
class ProducerWritesToOneSubscriberReadsFromAllTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  lazy val srv = TestStorageServer.getNewClean()

  override def beforeAll(): Unit = {
    srv
    createNewStream()
  }

  it should "handle all transactions produced by producer" in {
    val l = new CountDownLatch(1)
    val TRANSACTIONS_AMOUNT = 10000

    var subscriberTransactionsAmount = 0
    val producer = f.getProducer(
      name = "test_producer",
      partitions = Set(0))

    val s = f.getSubscriber(name = "sv2",
      partitions = Set(0, 1, 2),
      offset = Newest,
      useLastOffset = true,
      callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => this.synchronized {
        subscriberTransactionsAmount += 1
        if (subscriberTransactionsAmount == TRANSACTIONS_AMOUNT)
          l.countDown()
      })
    s.start()
    for (it <- 0 until TRANSACTIONS_AMOUNT) {
      val transaction = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)
      transaction.send("test")
      transaction.checkpoint()
    }
    producer.stop()
    l.await(1000, TimeUnit.MILLISECONDS)
    s.stop()
    subscriberTransactionsAmount shouldBe TRANSACTIONS_AMOUNT
  }

  override def afterAll(): Unit = {
    TestStorageServer.dispose(srv)
    onAfterAll()
  }
}

