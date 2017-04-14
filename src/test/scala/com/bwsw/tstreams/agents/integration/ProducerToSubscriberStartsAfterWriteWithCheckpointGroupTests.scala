package com.bwsw.tstreams.agents.integration

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.agents.group.CheckpointGroup
import com.bwsw.tstreams.agents.producer.{NewTransactionProducerPolicy, ProducerTransaction}
import com.bwsw.tstreams.env.ConfigurationOptions
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer
class ProducerToSubscriberStartsAfterWriteWithCheckpointGroupTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  f.setProperty(ConfigurationOptions.Stream.name, "test_stream").
    setProperty(ConfigurationOptions.Stream.partitionsCount, 3).
    setProperty(ConfigurationOptions.Stream.ttlSec, 60 * 10).
    setProperty(ConfigurationOptions.Coordination.connectionTimeoutMs, 7000).
    setProperty(ConfigurationOptions.Coordination.sessionTimeoutMs, 7000).
    setProperty(ConfigurationOptions.Producer.transportTimeoutMs, 5000).
    setProperty(ConfigurationOptions.Producer.Transaction.ttlMs, 6000).
    setProperty(ConfigurationOptions.Producer.Transaction.keepAliveMs, 2000).
    setProperty(ConfigurationOptions.Consumer.transactionPreload, 10).
    setProperty(ConfigurationOptions.Consumer.Subscriber.pollingFrequencyDelayMs, 100).
    setProperty(ConfigurationOptions.Consumer.dataPreload, 50)

  val srv = TestStorageServer.get()
  val storageClient = f.getStorageClient()
  storageClient.createStream("test_stream", 3, 24 * 3600, "")
  storageClient.shutdown()

  val COUNT = 10

  it should s"The producer sends $COUNT transactions, subscriber receives $COUNT when started after." +
    s"Then do group checkpoint and start new Subscriber from checkpointed place" in {
    val group = new CheckpointGroup()

    val bp = ListBuffer[Long]()
    val bs = ListBuffer[Long]()

    val lp = new CountDownLatch(1)
    val ls = new CountDownLatch(1)

    val producer = f.getProducer(
      name = "test_producer1",
      partitions = Set(0))

    val subscriber = f.getSubscriber(name = "ss+2",
      partitions = Set(0),
      offset = Oldest,
      useLastOffset = true,
      callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => this.synchronized {
        bs.append(transaction.getTransactionID())
        consumer.setStreamPartitionOffset(transaction.getPartition(), transaction.getTransactionID())
        if (bs.size == COUNT) {
          ls.countDown()
        }
      })

    val t = new Thread(() => {
      logger.info(s"Producer is master of partition: ${producer.isMasterOfPartition(0)}")
      lp.countDown()
    })

    for (i <- 0 until COUNT) {
      val t: ProducerTransaction = producer.newTransaction(policy = NewTransactionProducerPolicy.CheckpointIfOpened)
      t.send("test")
      t.checkpoint()

      bp.append(t.getTransactionID())
    }

    producer.stop()

    val lastTxn = bp.sorted.last
    println(lastTxn)
    val l = new CountDownLatch(1)
    srv.notifyConsumerTransactionCompleted(ct => lastTxn == ct.transactionID, l.countDown())

    group.add(subscriber)
    subscriber.start()
    ls.await(10, TimeUnit.SECONDS) shouldBe true
    group.checkpoint()
    subscriber.stop()
    bs.size shouldBe COUNT

    l.await(10, TimeUnit.SECONDS) shouldBe true

    val bs2 = ListBuffer[Long]()
    val ls2 = new CountDownLatch(1)

    val s2 = f.getSubscriber(name = "ss+2",
      partitions = Set(0),
      offset = Oldest,
      useLastOffset = true,
      callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => this.synchronized {
        bs2.append(transaction.getTransactionID())
        if (bs2.size == COUNT) {
          ls2.countDown()
        }
      })

    s2.start()
    ls2.await(10, TimeUnit.SECONDS) shouldBe false
    s2.stop()
    bs2.size shouldBe 0
  }

  override def afterAll(): Unit = {
    TestStorageServer.dispose(srv)
    onAfterAll()
  }
}
