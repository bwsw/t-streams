package agents.integration

/**
  * Created by mendelbaum_ma on 08.09.16.
  */

import java.util.UUID
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.consumer.Offset.Newest
import com.bwsw.tstreams.agents.consumer.subscriber.Callback
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.env.TSF_Dictionary
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{LocalGeneratorCreator, TestUtils}

import scala.collection.mutable.ListBuffer


class SubscriberWithTwoProducersFirstCancelSecondCheckpointTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  f.setProperty(TSF_Dictionary.Stream.NAME, "test_stream").
    setProperty(TSF_Dictionary.Stream.PARTITIONS, 3).
    setProperty(TSF_Dictionary.Stream.TTL, 60 * 10).
    setProperty(TSF_Dictionary.Coordination.CONNECTION_TIMEOUT, 7).
    setProperty(TSF_Dictionary.Coordination.TTL, 7).
    setProperty(TSF_Dictionary.Producer.TRANSPORT_TIMEOUT, 5).
    setProperty(TSF_Dictionary.Producer.Transaction.TTL, 3).
    setProperty(TSF_Dictionary.Producer.Transaction.KEEP_ALIVE, 1).
    setProperty(TSF_Dictionary.Consumer.TRANSACTION_PRELOAD, 10).
    setProperty(TSF_Dictionary.Consumer.DATA_PRELOAD, 10)
  it should "Integration MixIn checkpoint and cancel must be correctly processed on Subscriber " in {

    val bp1 = ListBuffer[UUID]()
    val bp2 = ListBuffer[UUID]()
    val bs = ListBuffer[UUID]()

    val lp2 = new CountDownLatch(1)
    val ls = new CountDownLatch(1)

    val producer1 = f.getProducer[String](
      name = "test_producer1",
      transactionGenerator = LocalGeneratorCreator.getGen(),
      converter = stringToArrayByteConverter,
      partitions = Set(0),
      isLowPriority = false)

    val producer2 = f.getProducer[String](
      name = "test_producer2",
      transactionGenerator = LocalGeneratorCreator.getGen(),
      converter = stringToArrayByteConverter,
      partitions = Set(0),
      isLowPriority = false)

    val subscriber = f.getSubscriber[String](name = "ss+2",
      transactionGenerator = LocalGeneratorCreator.getGen(),
      converter = arrayByteToStringConverter,
      partitions = Set(0),
      offset = Newest,
      isUseLastOffset = true,
      callback = new Callback[String] {
        override def onTransaction(consumer: TransactionOperator[String], transaction: ConsumerTransaction[String]): Unit = this.synchronized {
          bs.append(transaction.getTransactionUUID())
          ls.countDown()
        }
      })

    val t1 = new Thread(new Runnable {
      override def run(): Unit = {
        val transaction = producer1.newTransaction(policy = NewTransactionProducerPolicy.CheckpointIfOpened)
        lp2.countDown()
        bp1.append(transaction.getTransactionUUID())
        transaction.send("test")
        transaction.cancel()
      }
    })

    val t2 = new Thread(new Runnable {
      override def run(): Unit = {
        lp2.await()
        val t = producer2.newTransaction(policy = NewTransactionProducerPolicy.CheckpointIfOpened)
        bp2.append(t.getTransactionUUID())
        t.send("test")
        t.checkpoint()
      }
    })

    subscriber.start()

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    ls.await(10, TimeUnit.SECONDS)

    producer1.stop()
    producer2.stop()

    subscriber.stop()

    bs.size shouldBe 1 // Adopted by only one and it is from second
    bp2.head shouldBe bs.head
  }

  override def afterAll(): Unit = {
    onAfterAll()
  }
}