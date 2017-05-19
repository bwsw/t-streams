package com.bwsw.tstreams.agents.integration

import java.util.concurrent.CountDownLatch

import com.bwsw.tstreams.agents.producer._
import com.bwsw.tstreams.testutils._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer


class ProducerUsedBySeveralThreadsSimultaneouslyTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  val PARTITIONS_COUNT = 4
  val COUNT = 10000
  val THREADS = 8

  lazy val srv = TestStorageServer.getNewClean()
  lazy val storageClient = f.getStorageClient()

  lazy val producer = f.getProducer(
    name = "test_producer",
    partitions = (0 until PARTITIONS_COUNT).toSet)

  override def beforeAll(): Unit = {
    srv
    createNewStream(partitions = PARTITIONS_COUNT)
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
              producerAccumulator.append(t.getTransactionID)
            }
          })
        l.countDown()
      }))

    val start = System.currentTimeMillis()
    threads.foreach(_.start())
    l.await()
    val end = System.currentTimeMillis()
    //println(end - start)
    threads.foreach(_.join())
    producerAccumulator.size shouldBe COUNT * THREADS

  }

  it should "work correctly if two different threads uses different partitions (isolated partitions)" in {
    val l = new CountDownLatch(PARTITIONS_COUNT)
    val producerAccumulator = new ListBuffer[Long]()

    val threads = (0 until PARTITIONS_COUNT).map(partition => new Thread(() => {
      (0 until COUNT)
        .foreach(i => {
          val t = producer.newTransaction(NewProducerTransactionPolicy.CheckpointIfOpened, partition)
          t.send("data")
          t.checkpoint()
          producerAccumulator.synchronized {
            producerAccumulator.append(t.getTransactionID)
          }
        })
      l.countDown()
    }))

    val start = System.currentTimeMillis()
    threads.foreach(_.start())
    l.await()
    val end = System.currentTimeMillis()
    //println(end - start)
    threads.foreach(_.join())
    producerAccumulator.size shouldBe COUNT * PARTITIONS_COUNT
  }


  override def afterAll(): Unit = {
    producer.stop()
    TestStorageServer.dispose(srv)
    onAfterAll()
  }
}
