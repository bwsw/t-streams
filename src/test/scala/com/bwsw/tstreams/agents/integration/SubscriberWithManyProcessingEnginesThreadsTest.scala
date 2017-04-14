package com.bwsw.tstreams.agents.integration

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.consumer.Offset.Newest
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.env.ConfigurationOptions
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.util.Random

/**
  * Created by Ivan Kudryavtsev on 08.09.16.
  */
class SubscriberWithManyProcessingEnginesThreadsTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  val TOTAL_TRANSACTIONS = 10000
  val TOTAL_ITEMS = 1
  val TOTAL_PARTITIONS = 100
  val PARTITIONS = (0 until TOTAL_PARTITIONS).toSet
  val PROCESSING_ENGINES_THREAD_POOL = 10
  val TRANSACTION_BUFFER_THREAD_POOL = 10

  val POLLING_FREQUENCY_DELAY_MS = 1000

  f.setProperty(ConfigurationOptions.Stream.ttlSec, 60 * 10).
    setProperty(ConfigurationOptions.Coordination.connectionTimeoutMs, 7000).
    setProperty(ConfigurationOptions.Coordination.sessionTimeoutMs, 7000).
    setProperty(ConfigurationOptions.Producer.transportTimeoutMs, 5000).
    setProperty(ConfigurationOptions.Producer.Transaction.ttlMs, 6000).
    setProperty(ConfigurationOptions.Producer.Transaction.keepAliveMs, 2000).
    setProperty(ConfigurationOptions.Consumer.transactionPreload, 500).
    setProperty(ConfigurationOptions.Consumer.dataPreload, 10).
    setProperty(ConfigurationOptions.Stream.name, "test_stream").
    setProperty(ConfigurationOptions.Consumer.Subscriber.pollingFrequencyDelayMs, POLLING_FREQUENCY_DELAY_MS).
    setProperty(ConfigurationOptions.Consumer.Subscriber.processingEnginesThreadPoolSize, PROCESSING_ENGINES_THREAD_POOL).
    setProperty(ConfigurationOptions.Consumer.Subscriber.transactionBufferThreadPoolSize, TRANSACTION_BUFFER_THREAD_POOL).
    setProperty(ConfigurationOptions.Stream.partitionsCount, TOTAL_PARTITIONS)

  val srv = TestStorageServer.get()
  val storageClient = f.getStorageClient()
  storageClient.createStream("test_stream", TOTAL_PARTITIONS, 24 * 3600, "")
  storageClient.shutdown()

  it should s"Start and work correctly with PROCESSING_ENGINES_THREAD_POOL=$PROCESSING_ENGINES_THREAD_POOL" in {
    val awaitTransactionsLatch = new CountDownLatch(1)
    var transactionsCounter = 0

    val subscriber = f.getSubscriber(
      name = "test_subscriber", // name of the subscribing consumer
      partitions = PARTITIONS, // active partitions
      offset = Newest, // it will start from newest available partitions
      useLastOffset = false, // will ignore history
      callback = (op: TransactionOperator, transaction: ConsumerTransaction) => this.synchronized {
        transactionsCounter += 1
        if (transactionsCounter % 1000 == 0) {
          logger.info(s"I have read $transactionsCounter transactions up to now.")
          op.checkpoint()
        }
        if (transactionsCounter == TOTAL_TRANSACTIONS) // if the producer sent all information, then end
          awaitTransactionsLatch.countDown()
      })

    subscriber.start() // start subscriber to operate

    val producerThread = new Thread(() => {
      // create producer
      val producer = f.getProducer(
        name = "test_producer", // name of the producer
        partitions = PARTITIONS) // agent can be a master

      (0 until TOTAL_TRANSACTIONS).foreach(
        i => {
          val t = producer.newTransaction(policy = NewTransactionProducerPolicy.CheckpointIfOpened) // create new transaction
          (0 until TOTAL_ITEMS).foreach(j => {
            val v = Random.nextInt()
            t.send(s"$v")
          })
          t.checkpoint(false) // checkpoint the transaction
          if (i % 1000 == 0) {
            logger.info(s"I have wrote $i transactions up to now.")
          }
        })
      producer.stop() // stop operation
    })

    producerThread.start()
    producerThread.join()
    awaitTransactionsLatch.await(POLLING_FREQUENCY_DELAY_MS + 1000, TimeUnit.MILLISECONDS)
    subscriber.stop() // stop operation
    transactionsCounter shouldBe TOTAL_TRANSACTIONS
  }

  override def afterAll(): Unit = {
    TestStorageServer.dispose(srv)
    onAfterAll()
  }

}
