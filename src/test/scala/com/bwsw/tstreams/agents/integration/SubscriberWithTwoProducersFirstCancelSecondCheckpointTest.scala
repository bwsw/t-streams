package com.bwsw.tstreams.agents.integration

/**
  * Created by mendelbaum_ma on 08.09.16.
  */

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.consumer.Offset.Newest
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.agents.producer.NewProducerTransactionPolicy
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer


class SubscriberWithTwoProducersFirstCancelSecondCheckpointTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  lazy val srv = TestStorageServer.get()

  override def beforeAll(): Unit = {
    srv
    createNewStream()
  }

  it should "Integration MixIn checkpoint and cancel must be correctly processed on Subscriber " in {

    val bp1 = ListBuffer[Long]()
    val bp2 = ListBuffer[Long]()
    val bs = ListBuffer[Long]()

    val lp2 = new CountDownLatch(1)
    val ls = new CountDownLatch(1)

    val subscriber = f.getSubscriber(name = "ss+2",
      partitions = Set(0),
      offset = Newest,
      useLastOffset = true,
      callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => this.synchronized {
        bs.append(transaction.getTransactionID)
        ls.countDown()
      })

    subscriber.start()

    val producer1 = f.getProducer(
      name = "test_producer1",
      partitions = Set(0))

    val producer2 = f.getProducer(
      name = "test_producer2",
      partitions = Set(0))



    val t1 = new Thread(() => {
      val transaction = producer1.newTransaction(policy = NewProducerTransactionPolicy.CheckpointIfOpened)
      lp2.countDown()
      bp1.append(transaction.getTransactionID)
      transaction.send("test")
      transaction.cancel()
    })

    val t2 = new Thread(() => {
      lp2.await()
      val transaction = producer2.newTransaction(policy = NewProducerTransactionPolicy.CheckpointIfOpened)
      bp2.append(transaction.getTransactionID)
      transaction.send("test")
      transaction.checkpoint()
    })

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    ls.await(10, TimeUnit.SECONDS)

    producer1.stop()
    producer2.stop()

    subscriber.stop()

    bs.size shouldBe 1 // Adopted by only one and it is from second
    bp2.head shouldBe bs.head
  }

  override def afterAll(): Unit = {
    TestStorageServer.dispose(srv)
    onAfterAll()
  }
}