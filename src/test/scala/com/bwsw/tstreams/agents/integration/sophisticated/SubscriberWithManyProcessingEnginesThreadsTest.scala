package com.bwsw.tstreams.agents.integration.sophisticated

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.consumer.Offset.Newest
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.agents.producer.NewProducerTransactionPolicy
import com.bwsw.tstreams.env.ConfigurationOptions
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer
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

  val POLLING_FREQUENCY_DELAY_MS = 5000

  lazy val srv = TestStorageServer.getNewClean()

  override def beforeAll(): Unit = {
    f.setProperty(ConfigurationOptions.Consumer.Subscriber.pollingFrequencyDelayMs, POLLING_FREQUENCY_DELAY_MS)
      .setProperty(ConfigurationOptions.Consumer.Subscriber.processingEnginesThreadPoolSize, PROCESSING_ENGINES_THREAD_POOL)
      .setProperty(ConfigurationOptions.Consumer.Subscriber.transactionBufferThreadPoolSize, TRANSACTION_BUFFER_THREAD_POOL)

    srv
    createNewStream(partitions = TOTAL_PARTITIONS)
  }

  it should s"Start and work correctly with PROCESSING_ENGINES_THREAD_POOL=$PROCESSING_ENGINES_THREAD_POOL" in {
    val awaitTransactionsLatch = new CountDownLatch(1)
    var transactionsCounter = 0
    val producerTransactions = new ListBuffer[(Int, Long)]()
    val subscriberTransactions = new ListBuffer[(Int, Long)]()

    val subscriber = f.getSubscriber(
      name = "test_subscriber", // name of the subscribing consumer
      partitions = PARTITIONS, // active partitions
      offset = Newest, // it will start from newest available partitions
      useLastOffset = false, // will ignore history
      callback = (op: TransactionOperator, transaction: ConsumerTransaction) => this.synchronized {
        subscriberTransactions.append((transaction.getPartition, transaction.getTransactionID))
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
          val t = producer.newTransaction(policy = NewProducerTransactionPolicy.CheckpointIfOpened) // create new transaction
          (0 until TOTAL_ITEMS).foreach(j => {
            val v = Random.nextInt()
            t.send(s"$v")
          })
          producerTransactions.append((t.getPartition, t.getTransactionID))
          t.checkpoint() // checkpoint the transaction
          if ((i+1) % 1000 == 0) {
            logger.info(s"I have wrote ${i+1} transactions up to now.")
          }
        })
      producer.stop() // stop operation
    })

    val start = System.currentTimeMillis()
    producerThread.start()
    producerThread.join()
    val end = System.currentTimeMillis()
    println(end - start)
    awaitTransactionsLatch.await(POLLING_FREQUENCY_DELAY_MS + 1000, TimeUnit.MILLISECONDS)
    subscriber.stop() // stop operation
    (producerTransactions.toSet -- subscriberTransactions.toSet).isEmpty shouldBe true
  }

  override def afterAll(): Unit = {
    TestStorageServer.dispose(srv)
    onAfterAll()
  }

}
