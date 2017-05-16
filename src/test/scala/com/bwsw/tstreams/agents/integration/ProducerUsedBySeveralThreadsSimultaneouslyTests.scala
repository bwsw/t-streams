package com.bwsw.tstreams.agents.integration

import java.util.concurrent.CountDownLatch

import com.bwsw.tstreams.agents.producer._
import com.bwsw.tstreams.env.ConfigurationOptions
import com.bwsw.tstreams.testutils._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer


class ProducerUsedBySeveralThreadsSimultaneouslyTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  val ALL_PARTITIONS = 4
  val COUNT = 10000
  val THREADS = 8

  lazy val srv = TestStorageServer.get()
  lazy val storageClient = f.getStorageClient()

  lazy val producer = f.getProducer(
    name = "test_producer",
    partitions = (0 until ALL_PARTITIONS).toSet)

  override def beforeAll(): Unit = {
    f.setProperty(ConfigurationOptions.Stream.name, "test_stream").
      setProperty(ConfigurationOptions.Stream.partitionsCount, ALL_PARTITIONS).
      setProperty(ConfigurationOptions.Stream.ttlSec, 60 * 10).
      setProperty(ConfigurationOptions.Coordination.connectionTimeoutMs, 7000).
      setProperty(ConfigurationOptions.Coordination.sessionTimeoutMs, 7000).
      setProperty(ConfigurationOptions.Producer.transportTimeoutMs, 5000).
      setProperty(ConfigurationOptions.Producer.Transaction.ttlMs, 6000).
      setProperty(ConfigurationOptions.Producer.Transaction.keepAliveMs, 2000).
      setProperty(ConfigurationOptions.Consumer.transactionPreload, 10).
      setProperty(ConfigurationOptions.Consumer.dataPreload, 10)

    srv

    if(storageClient.checkStreamExists("test_stream"))
      storageClient.deleteStream("test_stream")

    storageClient.createStream("test_stream", ALL_PARTITIONS, 24 * 3600, "")
    storageClient.shutdown()
  }

  it should "work correctly if two different threads uses different partitions (mixed partitions)" in {
    val l = new CountDownLatch(THREADS)
    val producerAccumulator = new ListBuffer[Long]()

    val threads = (0 until THREADS).map(_ => new Thread(() => {
        (0 until COUNT)
          .foreach(i => {
            val t = producer.newTransaction(NewProducerTransactionPolicy.EnqueueIfOpened)
            t.send("data")
            t.checkpoint()
            producerAccumulator.synchronized {
              producerAccumulator.append(t.getTransactionID())
            }
          })
        l.countDown()
      }))

    val start = System.currentTimeMillis()
    threads.foreach(_.start())
    l.await()
    val end = System.currentTimeMillis()
    println(end - start)
    threads.foreach(_.join())
    producerAccumulator.size shouldBe COUNT * THREADS

  }

  it should "work correctly if two different threads uses different partitions (isolated partitions)" in {
    val l = new CountDownLatch(ALL_PARTITIONS)
    val producerAccumulator = new ListBuffer[Long]()

    val threads = (0 until ALL_PARTITIONS).map(partition => new Thread(() => {
      (0 until COUNT)
        .foreach(i => {
          val t = producer.newTransaction(NewProducerTransactionPolicy.CheckpointAsyncIfOpened, partition)
          t.send("data")
          t.checkpoint()
          producerAccumulator.synchronized {
            producerAccumulator.append(t.getTransactionID())
          }
        })
      l.countDown()
    }))

    val start = System.currentTimeMillis()
    threads.foreach(_.start())
    l.await()
    val end = System.currentTimeMillis()
    println(end - start)
    threads.foreach(_.join())
    producerAccumulator.size shouldBe COUNT * ALL_PARTITIONS
  }


  override def afterAll(): Unit = {
    producer.stop()
    TestStorageServer.dispose(srv)
    onAfterAll()
  }
}
