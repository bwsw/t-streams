package agents.integration

import java.util.concurrent.CountDownLatch

import com.bwsw.tstreams.agents.consumer.Offset.Newest
import com.bwsw.tstreams.agents.consumer.subscriber.Callback
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.env.TSF_Dictionary
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{LocalGeneratorCreator, TestUtils}

import scala.collection.mutable.ListBuffer

/**
  * Created by Mikhail Mendelbaum on 02.09.16.
  */
class IntersectingTransactionsTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

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



  it should "handle all transactions produced by two different producers, the first ends first started " in {
    val bp = ListBuffer[Long]()
    val bs = ListBuffer[Long]()
    val lp1 = new CountDownLatch(1)
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

    val s = f.getSubscriber[String](name = "ss+2",
      transactionGenerator = LocalGeneratorCreator.getGen(),
      converter = arrayByteToStringConverter,
      partitions = Set(0),
      offset = Newest,
      isUseLastOffset = true,
      callback = new Callback[String] {
        override def onTransaction(consumer: TransactionOperator[String], transaction: ConsumerTransaction[String]): Unit = this.synchronized {
          bs.append(transaction.getTransactionID())
          if (bs.size == 2) {
            ls.countDown()
          }
        }
      })

    val t1 = new Thread(new Runnable {
      override def run(): Unit = {
        val t = producer1.newTransaction(policy = NewTransactionProducerPolicy.CheckpointIfOpened)
        bp.append(t.getTransactionID())
        lp2.countDown()
        lp1.await()
        t.send("test")
        t.checkpoint()
      }
    })

    val t2 = new Thread(new Runnable {
      override def run(): Unit = {
        lp2.await()
        val t = producer2.newTransaction(policy = NewTransactionProducerPolicy.CheckpointIfOpened)
        bp.append(t.getTransactionID())
        t.send("test")
        t.checkpoint()
        lp1.countDown()
      }
    })

    s.start()

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    ls.await()

    producer1.stop()
    producer2.stop()
    s.stop()

    bp.head shouldBe bs.head
    bp.tail.head shouldBe bs.tail.head
  }

  override def afterAll(): Unit = {
    onAfterAll()
  }

}
