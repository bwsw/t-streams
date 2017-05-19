package com.bwsw.tstreams.agents.integration

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.agents.group.CheckpointGroup
import com.bwsw.tstreams.agents.producer.{NewProducerTransactionPolicy, ProducerTransaction}
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer

class ProducerToSubscriberStartsAfterWriteWithCheckpointGroupTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  val COUNT = 10

  lazy val srv = TestStorageServer.getNewClean()

  override def beforeAll(): Unit = {
    srv
    createNewStream()
  }

  it should s"The producer sends $COUNT transactions, subscriber receives $COUNT of them when started after." +
    s"Then do group checkpoint and start new Subscriber from checkpointed place" in {
    val group = new CheckpointGroup()

    val bp = ListBuffer[Long]()
    val bs = ListBuffer[Long]()

    val subscriber1Latch = new CountDownLatch(1)

    val producer = f.getProducer(
      name = "test_producer1",
      partitions = Set(0))

    val subscriber = f.getSubscriber(name = "ss+2",
      partitions = Set(0),
      offset = Oldest,
      useLastOffset = true,
      callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => this.synchronized {
        bs.append(transaction.getTransactionID)
        consumer.setStreamPartitionOffset(transaction.getPartition, transaction.getTransactionID)
        if (bs.size == COUNT) {
          subscriber1Latch.countDown()
        }
      })

    for (i <- 0 until COUNT) {
      val t: ProducerTransaction = producer.newTransaction(policy = NewProducerTransactionPolicy.CheckpointIfOpened)
      t.send("test")
      t.checkpoint()
      bp.append(t.getTransactionID)
    }

    producer.stop()

    val lastTxn = bp.sorted.last
    val ttsSynchronizationLatch = new CountDownLatch(1)
    srv.notifyConsumerTransactionCompleted(ct => lastTxn == ct.transactionID, ttsSynchronizationLatch.countDown())

    group.add(subscriber)
    subscriber.start()
    subscriber1Latch.await(10, TimeUnit.SECONDS) shouldBe true
    group.checkpoint()
    subscriber.stop()
    bs.size shouldBe COUNT

    ttsSynchronizationLatch.await(10, TimeUnit.SECONDS) shouldBe true

    val bs2 = ListBuffer[Long]()
    val subscriber2Latch = new CountDownLatch(1)

    val s2 = f.getSubscriber(name = "ss+2",
      partitions = Set(0),
      offset = Oldest,
      useLastOffset = true,
      callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => this.synchronized {
        subscriber2Latch.countDown()
      })
    s2.start()
    subscriber2Latch.await(5, TimeUnit.SECONDS) shouldBe false
    s2.stop()
  }

  override def afterAll(): Unit = {
    TestStorageServer.dispose(srv)
    onAfterAll()
  }
}
