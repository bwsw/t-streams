package com.bwsw.tstreams.agents.integration.sophisticated

/**
  * Created by Ivan Kudryavtsev on 21.09.16.
  */

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.consumer.Offset.Newest
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.agents.producer.{NewTransactionProducerPolicy, Producer}
import com.bwsw.tstreams.env.{ConfigurationOptions, TStreamsFactory}
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer
import scala.util.Random

class ProducerMasterChangeComplexTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  val MAX_NEW_TXN_RETRY = 10
  val PRODUCERS_AMOUNT = 10
  val TRANSACTIONS_AMOUNT_EACH = 1000
  val PROBABILITY = 0.01
  val PARTITIONS_COUNT = 10
  val PARTITIONS = (0 until PARTITIONS_COUNT).toSet
  val MAX_WAIT_AFTER_ALL_PRODUCERS = 5

  val onCompleteLatch = new CountDownLatch(PRODUCERS_AMOUNT)
  val waitCompleteLatch = new CountDownLatch(1)

  lazy val srv = TestStorageServer.get()
  lazy val storageClient = f.getStorageClient()
  lazy val producerBuffer = PARTITIONS.toList.map(_ => ListBuffer[Long]()).toArray
  lazy val subscriberBuffer = PARTITIONS.toList.map(_ => ListBuffer[Long]()).toArray

  override def beforeAll(): Unit = {
    f.setProperty(ConfigurationOptions.Stream.name, "test_stream").
      setProperty(ConfigurationOptions.Stream.partitionsCount, PARTITIONS_COUNT).
      setProperty(ConfigurationOptions.Stream.ttlSec, 60 * 10).
      setProperty(ConfigurationOptions.Coordination.connectionTimeoutMs, 4000).
      setProperty(ConfigurationOptions.Coordination.sessionTimeoutMs, 4000).
      setProperty(ConfigurationOptions.Producer.transportTimeoutMs, 2000).
      setProperty(ConfigurationOptions.Producer.Transaction.ttlMs, 500).
      setProperty(ConfigurationOptions.Producer.Transaction.keepAliveMs, 100).
      setProperty(ConfigurationOptions.Consumer.transactionPreload, 500).
      setProperty(ConfigurationOptions.Consumer.dataPreload, 10)

    srv
    storageClient.createStream("test_stream", PARTITIONS_COUNT, 24 * 3600, "")
    storageClient.shutdown()
  }

  class ProducerWorker(val factory: TStreamsFactory, val onCompleteLatch: CountDownLatch, val amount: Int, val probability: Double, id: Int) {
    var producer: Producer = null
    var counter: Int = 0

    val intFactory = factory.copy()
    intFactory.setProperty(ConfigurationOptions.Producer.bindPort, 40000 + id)

    def loop(partitions: Set[Int], checkpointModeSync: Boolean = true) = {
      while (counter < amount) {
        producer = makeNewProducer(partitions)

        while (probability < Random.nextDouble() && counter < amount) {
          val t = producer.newTransaction(policy = NewTransactionProducerPolicy.CheckpointIfOpened, -1)
          t.send("test".getBytes())
          t.checkpoint(checkpointModeSync)
          producerBuffer(t.getPartition()).synchronized {
            producerBuffer(t.getPartition()).append(t.getTransactionID())
          }
          counter += 1
        }
        producer.stop()
      }
      onCompleteLatch.countDown()
    }

    def run(partitions: Set[Int], checkpointModeSync: Boolean = true): Thread = {
      val thread = new Thread(() => loop(partitions, checkpointModeSync))
      thread.start()
      thread
    }

    // private - will not be called outside
    private def makeNewProducer(partitions: Set[Int]) = {
      intFactory.getProducer(
        name = "test_producer1",
        partitions = partitions)
    }
  }


  var subscriberCounter = 0
  lazy val subscriber = f.getSubscriber(name = "s",
    partitions = PARTITIONS, // Set(0),
    offset = Newest,
    useLastOffset = false, // true
    callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => this.synchronized {
      subscriberCounter += 1
      subscriberBuffer(transaction.getPartition()).append(transaction.getTransactionID())
      if (subscriberCounter == PRODUCERS_AMOUNT * TRANSACTIONS_AMOUNT_EACH)
        waitCompleteLatch.countDown()
    })

  it should "handle multiple master change correctly" in {

    subscriber.start()

    val producersThreads = (0 until PRODUCERS_AMOUNT)
      .map(producer => new ProducerWorker(f, onCompleteLatch, TRANSACTIONS_AMOUNT_EACH, PROBABILITY, producer).run(PARTITIONS))

    onCompleteLatch.await()
    producersThreads.foreach(thread => thread.join())
    waitCompleteLatch.await(MAX_WAIT_AFTER_ALL_PRODUCERS, TimeUnit.SECONDS)
    subscriber.stop()

    //val consumer = f.getConsumer("cons", partitions = PARTITIONS, offset = Oldest, useLastOffset = false)

    //PARTITIONS.map(p => {
    //
    //})

    subscriberCounter shouldBe TRANSACTIONS_AMOUNT_EACH * PRODUCERS_AMOUNT

    PARTITIONS.foreach(p => {
      val intersectionSize = producerBuffer(p).toSet.intersect(subscriberBuffer(p).toSet).size
      intersectionSize shouldBe producerBuffer(p).size
      intersectionSize shouldBe subscriberBuffer(p).size
    })

  }

  override def afterAll() {
    TestStorageServer.dispose(srv)
    onAfterAll()
  }
}







