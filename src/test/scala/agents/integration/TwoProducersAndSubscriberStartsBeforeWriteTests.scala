package agents.integration

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.consumer.Offset.Newest
import com.bwsw.tstreams.agents.consumer.subscriber.Callback
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.env.ConfigurationOptions
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{LocalGeneratorCreator, TestUtils}

import scala.collection.mutable.ListBuffer

/**
  * Created by Ivan Kudryavtsev on 07.09.16.
  */
class TwoProducersAndSubscriberStartsBeforeWriteTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  f.setProperty(ConfigurationOptions.Stream.name, "test_stream").
    setProperty(ConfigurationOptions.Stream.partitionsCount, 3).
    setProperty(ConfigurationOptions.Stream.ttlSec, 60 * 10).
    setProperty(ConfigurationOptions.Coordination.connectionTimeoutMs, 7).
    setProperty(ConfigurationOptions.Coordination.sessionTimeoutMs, 7).
    setProperty(ConfigurationOptions.Producer.transportTimeoutMs, 5).
    setProperty(ConfigurationOptions.Producer.Transaction.ttlMs, 3).
    setProperty(ConfigurationOptions.Producer.Transaction.keepAliveMs, 1).
    setProperty(ConfigurationOptions.Consumer.transactionPreload, 10).
    setProperty(ConfigurationOptions.Consumer.dataPreload, 10)

  val COUNT = 1000

  it should s"Two producers send $COUNT transactions each, subscriber receives ${2 * COUNT} when started after." in {

    val bp = ListBuffer[Long]()
    val bs = ListBuffer[Long]()

    val lp2 = new CountDownLatch(1)
    val ls = new CountDownLatch(1)

    val producer1 = f.getProducer(
      name = "test_producer1",
      transactionGenerator = LocalGeneratorCreator.getGen(),
      partitions = Set(0))


    val producer2 = f.getProducer(
      name = "test_producer2",
      transactionGenerator = LocalGeneratorCreator.getGen(),
      partitions = Set(0))

    val s = f.getSubscriber(name = "ss+2",
      transactionGenerator = LocalGeneratorCreator.getGen(),
      partitions = Set(0),
      offset = Newest,
      useLastOffset = true,
      callback = new Callback {
        override def onTransaction(consumer: TransactionOperator[String], transaction: ConsumerTransaction[String]): Unit = this.synchronized {
          bs.append(transaction.getTransactionID())
          if (bs.size == 2 * COUNT) {
            ls.countDown()
          }
        }
      })

    val t1 = new Thread(() => {
      logger.info(s"Producer-1 is master of partition: ${producer1.isMasterOfPartition(0)}")
      for (i <- 0 until COUNT) {
        val t = producer1.newTransaction(policy = NewTransactionProducerPolicy.CheckpointIfOpened)
        bp.append(t.getTransactionID())
        lp2.countDown()
        t.send("test")
        t.checkpoint()
      }
    })
    val t2 = new Thread(() => {
      logger.info(s"Producer-2 is master of partition: ${producer2.isMasterOfPartition(0)}")
      for (i <- 0 until COUNT) {
        lp2.await()
        val t = producer2.newTransaction(policy = NewTransactionProducerPolicy.CheckpointIfOpened)
        bp.append(t.getTransactionID())
        t.send("test")
        t.checkpoint()
      }
    })
    s.start()

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    ls.await(10, TimeUnit.SECONDS)
    producer1.stop()
    producer2.stop()
    s.stop()
    bs.size shouldBe 2 * COUNT
  }


  override def afterAll(): Unit = {
    onAfterAll()
  }
}
