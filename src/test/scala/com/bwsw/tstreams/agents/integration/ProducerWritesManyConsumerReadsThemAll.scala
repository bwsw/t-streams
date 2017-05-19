package com.bwsw.tstreams.agents.integration

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.producer.NewProducerTransactionPolicy
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer

/**
  * Created by ivan on 22.04.17.
  */
class ProducerWritesManyConsumerReadsThemAll extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  lazy val srv = TestStorageServer.get()

  override def beforeAll(): Unit = {
    srv
    createNewStream()
  }


  it should "handle all transactions and do not loose them" in {
    val TRANSACTIONS_PER_PRODUCER = 10000

    val producer = f.getProducer(
      name = "test_producer",
      partitions = Set(0))

    val producerIterAcc = ListBuffer[Long]()

    (0 until TRANSACTIONS_PER_PRODUCER).foreach(_ => {
      val transaction = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)
      transaction.send("test")
      transaction.checkpoint()
      producerIterAcc.append(transaction.getTransactionID)
    })
    producer.stop()

    val lastProducerTransaction = producerIterAcc.last

    val consumer = f.getConsumer(name = "sv2",
      partitions = Set(0),
      offset = Oldest)

    consumer.start()

    val exitFlag = new AtomicBoolean(false)
    val consumerIterAcc = ListBuffer[Long]()
    val counter = new AtomicInteger(0)

    logger.info(s"Loading up to ${lastProducerTransaction}")
    producerIterAcc.foreach(tp => {
      val tOpt = consumer.getTransaction(0)
      tOpt.foreach(t => {
        consumerIterAcc.append(t.getTransactionID)
        t.getTransactionID shouldBe tp
      })
      if (counter.incrementAndGet() % 100 == 0)
        logger.info(s"${counter.get()} received")
    })

    logger.info(s"Completed loading up to ${lastProducerTransaction}")
    producerIterAcc shouldBe consumerIterAcc

    consumer.stop()
  }

  override def afterAll(): Unit = {
    TestStorageServer.dispose(srv)
    onAfterAll()
  }
}


