package com.bwsw.tstreams.agents.integration

import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.producer.NewProducerTransactionPolicy
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer

/**
  * Created by Ivan Kudryavtsev on 22.04.17.
  */
class ProducersComeAndLeaveConsumerReadsTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  lazy val server = TestStorageServer.getNewClean()

  override def beforeAll(): Unit = {
    server
    createNewStream()
  }

  it should "handle all transactions and do not loose them" in {
    val TRANSACTIONS_PER_PRODUCER = 100
    val PRODUCERS = 10

    val consumer = f.getConsumer(name = "sv2",
      partitions = Set(0),
      offset = Oldest)

    consumer.start()

    (0 until PRODUCERS).foreach(_ => {
      val producerTransactionAccumulator = ListBuffer[Long]()
      val producer = f.getProducer(name = "test_producer", partitions = Set(0))
      val exitFlag = new AtomicBoolean(false)
      val consumerTransactionAccumulator = ListBuffer[Long]()

      (0 until TRANSACTIONS_PER_PRODUCER).foreach(_ => {
        val transaction = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)
        transaction
          .send("test")
          .checkpoint()
        producerTransactionAccumulator.append(transaction.getTransactionID)
      })
      producer.stop()
      val lastProducerTransaction = producerTransactionAccumulator.last
      Thread.sleep(100)
      logger.info(s"Loading up to $lastProducerTransaction")
      while(!exitFlag.get()) {
        val tOpt = consumer.getTransaction(0)
        tOpt.foreach(t => {
          consumerTransactionAccumulator.append(t.getTransactionID)
          if(t.getTransactionID == lastProducerTransaction)
            exitFlag.set(true)
        })
      }
      logger.info(s"Completed loading up to $lastProducerTransaction")
      producerTransactionAccumulator shouldBe consumerTransactionAccumulator
    })
    consumer.stop()
  }

  override def afterAll(): Unit = {
    TestStorageServer.dispose(server)
    onAfterAll()
  }
}
