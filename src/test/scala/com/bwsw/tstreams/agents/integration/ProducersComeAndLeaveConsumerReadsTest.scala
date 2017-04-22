package com.bwsw.tstreams.agents.integration

import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.env.ConfigurationOptions
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer

/**
  * Created by ivan on 22.04.17.
  */
class ProducersComeAndLeaveConsumerReadsTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  f.setProperty(ConfigurationOptions.Stream.name, "test_stream").
    setProperty(ConfigurationOptions.Stream.partitionsCount, 3).
    setProperty(ConfigurationOptions.Stream.ttlSec, 60 * 10).
    setProperty(ConfigurationOptions.Coordination.connectionTimeoutMs, 7000).
    setProperty(ConfigurationOptions.Coordination.sessionTimeoutMs, 7000).
    setProperty(ConfigurationOptions.Producer.transportTimeoutMs, 5000).
    setProperty(ConfigurationOptions.Producer.Transaction.ttlMs, 500).
    setProperty(ConfigurationOptions.Producer.Transaction.keepAliveMs, 100).
    setProperty(ConfigurationOptions.Consumer.transactionPreload, 500).
    setProperty(ConfigurationOptions.Consumer.dataPreload, 10)

  val srv = TestStorageServer.get()
  val storageClient = f.getStorageClient()
  storageClient.createStream("test_stream", 3, 24 * 3600, "")
  storageClient.shutdown()

  it should "handle all transactions and do not loose them" in {
    val TRANSACTIONS_PER_PRODUCER = 1000
    val PRODUCERS = 1000

    val consumer = f.getConsumer(name = "sv2",
      partitions = Set(0),
      offset = Oldest)

    consumer.start()

    (0 until PRODUCERS).foreach(_ => {
      val producer = f.getProducer(
        name = "test_producer",
        partitions = Set(0))
      val producerIterAcc = ListBuffer[Long]()
      (0 until TRANSACTIONS_PER_PRODUCER).foreach(_ => {
        val transaction = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
        transaction.send("test")
        transaction.checkpoint()
        producerIterAcc.append(transaction.getTransactionID())
      })
      producer.stop()
      val lastProducerTransaction = producerIterAcc.last
      Thread.sleep(100)
      val exitFlag = new AtomicBoolean(false)
      val consumerIterAcc = ListBuffer[Long]()
      logger.info(s"Loading up to ${lastProducerTransaction}")
      while(!exitFlag.get()) {
        val tOpt = consumer.getTransaction(0)
        tOpt.foreach(t => {
          consumerIterAcc.append(t.getTransactionID())
          if(t.getTransactionID() == lastProducerTransaction)
            exitFlag.set(true)
        })
      }
      logger.info(s"Completed loading up to ${lastProducerTransaction}")
      producerIterAcc shouldBe consumerIterAcc
    })
    consumer.stop()
  }

  override def afterAll(): Unit = {
    TestStorageServer.dispose(srv)
    onAfterAll()
  }
}
