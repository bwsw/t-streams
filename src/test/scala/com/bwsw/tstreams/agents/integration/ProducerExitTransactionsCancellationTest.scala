package com.bwsw.tstreams.agents.integration

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.consumer.Offset.Newest
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.agents.producer.NewProducerTransactionPolicy
import com.bwsw.tstreams.env.ConfigurationOptions
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 19.05.17.
  */
class ProducerExitTransactionsCancellationTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  lazy val srv = TestStorageServer.getNewClean()

  override def beforeAll(): Unit = {
    srv
    createNewStream()
  }

  override def afterAll(): Unit = {
    TestStorageServer.dispose(srv)
    onAfterAll()
  }

  it should "subscriber skips fast transactions which are opened but not closed when the producer exits" in {
    val l = new CountDownLatch(2)

    val producer1 = f.getProducer(
      name = "producer1",
      partitions = Set(0))

    val producer2 = f.getProducer(
      name = "producer2",
      partitions = Set(0))

    val s = f.getSubscriber(name = "subscriber",
      partitions = Set(0),
      offset = Newest,
      useLastOffset = false,
      callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => l.countDown())

    s.start()

    producer1
      .newTransaction(policy = NewProducerTransactionPolicy.EnqueueIfOpened, 0)
      .send("")
      .checkpoint()

    producer1.newTransaction(policy = NewProducerTransactionPolicy.EnqueueIfOpened, 0)
    producer1.stop()

    producer2
      .newTransaction(policy = NewProducerTransactionPolicy.EnqueueIfOpened, 0)
      .send("")
      .checkpoint()

    producer2.stop()

    val delay = f.getProperty(ConfigurationOptions.Producer.Transaction.ttlMs).asInstanceOf[Int] / 2
    l.await(delay, TimeUnit.MILLISECONDS) shouldBe true

    s.stop()

  }
}
