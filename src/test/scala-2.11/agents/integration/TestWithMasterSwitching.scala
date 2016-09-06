package agents.integration

/**
  * Created by mendelbaum_ma on 06.09.16.
  */

import java.util.UUID
import java.util.concurrent.{CountDownLatch, TimeUnit}
import com.bwsw.tstreams.agents.consumer.Offset.Newest
import com.bwsw.tstreams.agents.consumer.TransactionOperator
import com.bwsw.tstreams.agents.consumer.subscriber.Callback
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.env.TSF_Dictionary
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{LocalGeneratorCreator, TestUtils}
import scala.collection.mutable.ListBuffer
/**
  * Created by mendelbaum_ma on 05.09.16.
  */
class TestWithMasterSwitching extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
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
  it should "switching the master after his hundred transactions " in {

    val bp = ListBuffer[UUID]()
    var bs = ListBuffer[UUID]()
    val lp2 = new CountDownLatch(1)
    val  ls = new  CountDownLatch(1)

    val producer1 = f.getProducer[String](
      name = "test_producer1",
      txnGenerator = LocalGeneratorCreator.getGen(),
      converter = stringToArrayByteConverter,
      partitions = Set(0),
      isLowPriority = false)


    val producer2 = f.getProducer[String](
      name = "test_producer2",
      txnGenerator = LocalGeneratorCreator.getGen(),
      converter = stringToArrayByteConverter,
      partitions = Set(0),
      isLowPriority = false)

    val s = f.getSubscriber[String](name = "ss+2",
      txnGenerator = LocalGeneratorCreator.getGen(),
      converter = arrayByteToStringConverter,
      partitions = Set(0),
      offset = Newest,
      isUseLastOffset = true,
      callback = new Callback[String] {
        override def onEvent(consumer: TransactionOperator[String], partition: Int, uuid: UUID, count: Int): Unit = this.synchronized {
          bs.append(uuid)
          if (bs.size == 1100) {
            ls.countDown()
          }
        }
      })
    val t1 = new Thread(new Runnable {
      override def run(): Unit = {
        for (i <- 0 until 100) {
          val t = producer1.newTransaction(policy = NewTransactionProducerPolicy.CheckpointIfOpened)
          bp.append(t.getTransactionUUID())
          lp2.countDown()
          t.send("test")
          t.checkpoint()
        }
        producer1.stop()
      }
    })
    val t2 = new Thread(new Runnable {
      override def run(): Unit = {
        for (i <- 0 until 1000) {
          lp2.await()
          val t = producer2.newTransaction(policy = NewTransactionProducerPolicy.CheckpointIfOpened)
          bp.append(t.getTransactionUUID())
          t.send("test")
          t.checkpoint()
        }
      }
    })
    s.start()
    t1.start()
    t2.start()

    t1.join()
    t2.join()

    ls.await(20, TimeUnit.SECONDS)
    producer2.stop()
    s.stop()
    bs.size shouldBe 1100
  }
  override def afterAll(): Unit = {
    onAfterAll()
  }
}

