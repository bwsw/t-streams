package com.bwsw.tstreams.agents.integration

/**
  * Created by Mendelbaum MA on 06.09.16.
  */

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.consumer.Offset.Newest
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.agents.producer.NewProducerTransactionPolicy
import com.bwsw.tstreams.env.ConfigurationOptions
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer


class ProducerMasterChangeTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  lazy val srv = TestStorageServer.get()
  lazy val storageClient = f.getStorageClient()

  override def beforeAll(): Unit = {
    f.setProperty(ConfigurationOptions.Stream.name, "test_stream").
      setProperty(ConfigurationOptions.Stream.partitionsCount, 3).
      setProperty(ConfigurationOptions.Stream.ttlSec, 60 * 10).
      setProperty(ConfigurationOptions.Coordination.connectionTimeoutMs, 7000).
      setProperty(ConfigurationOptions.Coordination.sessionTimeoutMs, 7000).
      setProperty(ConfigurationOptions.Producer.transportTimeoutMs, 5000).
      setProperty(ConfigurationOptions.Producer.Transaction.ttlMs, 6000).
      setProperty(ConfigurationOptions.Producer.Transaction.keepAliveMs, 2000).
      setProperty(ConfigurationOptions.Consumer.transactionPreload, 500).
      setProperty(ConfigurationOptions.Consumer.dataPreload, 10)

    srv
    storageClient.createStream("test_stream", 3, 24 * 3600, "")
    storageClient.shutdown()
  }

  it should "switching the master after 100 transactions " in {

    val bp = ListBuffer[Long]()
    val bs = ListBuffer[Long]()

    val lp2 = new CountDownLatch(1)
    val ls = new CountDownLatch(1)

    val producer1 = f.getProducer(
      name = "test_producer1",
      partitions = Set(0))


    val producer2 = f.getProducer(
      name = "test_producer2",
      partitions = Set(0))

    val s = f.getSubscriber(name = "ss+2",
      partitions = Set(0),
      offset = Newest,
      useLastOffset = false,
      callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => this.synchronized {
        bs.append(transaction.getTransactionID())
        if (bs.size == 1100) {
          ls.countDown()
        }
      })
    val t1 = new Thread(() => {
      logger.info(s"Producer-1 is master of partition: ${producer1.isMasterOfPartition(0)}")
      for (i <- 0 until 100) {
        val t = producer1.newTransaction(policy = NewProducerTransactionPolicy.CheckpointIfOpened)
        bp.synchronized {
          bp.append(t.getTransactionID())
        }
        lp2.countDown()
        t.send("test".getBytes())
        t.checkpoint()
      }
      producer1.stop()
    })
    val t2 = new Thread(() => {
      logger.info(s"Producer-2 is master of partition: ${producer2.isMasterOfPartition(0)}")
      for (i <- 0 until 1000) {
        lp2.await()
        val t = producer2.newTransaction(policy = NewProducerTransactionPolicy.CheckpointIfOpened)
        bp.synchronized {
          bp.append(t.getTransactionID())
        }
        t.send("test".getBytes())
        t.checkpoint()
      }
    })
    s.start()

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    ls.await(60, TimeUnit.SECONDS)
    producer2.stop()
    s.stop()
    bs.size shouldBe 1100

    bp.toSet.intersect(bs.toSet).size shouldBe 1100
  }

  override def afterAll() {
    TestStorageServer.dispose(srv)
    onAfterAll()
  }
}

