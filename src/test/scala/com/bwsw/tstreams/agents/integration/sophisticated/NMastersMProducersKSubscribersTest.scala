package com.bwsw.tstreams.agents.integration.sophisticated

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.consumer.Offset.Newest
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer

/**
  * Created by Ivan Kudryavtsev on 14.04.17.
  */
class NMastersMProducersKSubscribersTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  val ALL_PARTITIONS = 4
  val PRODUCER_COUNT = 2
  val SUBSCRIBER_COUNT = 2
  val TRANSACTION_COUNT = 10000

  lazy val srv = TestStorageServer.get()

  override def beforeAll(): Unit = {
    srv
    createNewStream(partitions = ALL_PARTITIONS)
  }

  it should s"Start ${ALL_PARTITIONS} masters (each for one partition), launch ${PRODUCER_COUNT} producers " +
    s"${SUBSCRIBER_COUNT} subscribers and deliver ${PRODUCER_COUNT * TRANSACTION_COUNT} transactions " +
    "to every subscriber" in {

    // start masters
    val masters = (0 until ALL_PARTITIONS)
      .map(partition => f.getProducer(name = s"m${partition}", partitions = Set(partition)))

    masters.foreach(m => m.newTransaction().cancel())

    val producerTransactions = ListBuffer[Long]()
    val producerThreads = (0 until PRODUCER_COUNT)
      .map(id => {
        val p = f.getProducer(name = s"p${id}", partitions = (0 until ALL_PARTITIONS).toSet)
        val t = new Thread(() => {
          (0 until TRANSACTION_COUNT).foreach(_ => {
            val t = p.newTransaction()
            producerTransactions.synchronized {
              producerTransactions.append(t.getTransactionID)
            }
            t.send("test".getBytes())
            t.checkpoint()
          })
          p.stop()
        })
        t
      })

    val subscriberAccumulators = (0 until SUBSCRIBER_COUNT).map(_ => ListBuffer[Long]()).toArray
    val subscribersLatch = new CountDownLatch(SUBSCRIBER_COUNT)

    val subscribers = (0 until SUBSCRIBER_COUNT).map(id => {
      f.getSubscriber(s"s${id}",
        partitions = (0 until ALL_PARTITIONS).toSet,
        offset = Newest,
        useLastOffset = false,
        callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => this.synchronized {
          subscriberAccumulators(id).append(transaction.getTransactionID)
          if (subscriberAccumulators(id).size == PRODUCER_COUNT * TRANSACTION_COUNT)
            subscribersLatch.countDown()
        })
    })

    subscribers.foreach(subscriber => subscriber.start())
    producerThreads.foreach(t => t.start())

    subscribersLatch.await(200, TimeUnit.SECONDS) shouldBe true
    subscriberAccumulators.foreach(acc => acc.sorted shouldBe producerTransactions.sorted)

    producerThreads.foreach(t => t.join())
    subscribers.foreach(subscriber => subscriber.stop())
    masters.foreach(master => master.stop())
  }

  override def afterAll(): Unit = {
    onAfterAll()
    TestStorageServer.dispose(srv)
  }
}
