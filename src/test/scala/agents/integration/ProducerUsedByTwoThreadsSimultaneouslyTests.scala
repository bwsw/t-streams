package agents.integration

import java.util.concurrent.CountDownLatch

import com.bwsw.tstreams.agents.producer._
import com.bwsw.tstreams.env.TSF_Dictionary
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._


class ProducerUsedByTwoThreadsSimultaneouslyTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {


  // keep it greater than 3
  val ALL_PARTITIONS = 2
  val COUNT = 10000

  f.setProperty(TSF_Dictionary.Stream.NAME, "test_stream").
    setProperty(TSF_Dictionary.Stream.PARTITIONS, ALL_PARTITIONS).
    setProperty(TSF_Dictionary.Stream.TTL, 60 * 10).
    setProperty(TSF_Dictionary.Coordination.CONNECTION_TIMEOUT, 7).
    setProperty(TSF_Dictionary.Coordination.TTL, 7).
    setProperty(TSF_Dictionary.Producer.TRANSPORT_TIMEOUT, 5).
    setProperty(TSF_Dictionary.Producer.Transaction.TTL, 6).
    setProperty(TSF_Dictionary.Producer.Transaction.KEEP_ALIVE, 2).
    setProperty(TSF_Dictionary.Consumer.TRANSACTION_PRELOAD, 10).
    setProperty(TSF_Dictionary.Consumer.DATA_PRELOAD, 10)

  val producer = f.getProducer[String](
    name = "test_producer",
    transactionGenerator = LocalGeneratorCreator.getGen(),
    converter = stringToArrayByteConverter,
    partitions = (0 until ALL_PARTITIONS).toSet)

  it should "work correctly if two different threads uses different partitions" in {
    val l = new CountDownLatch(2)

    val t1 = new Thread(new Runnable {
      override def run(): Unit = {
        (0 until COUNT)
          .foreach(i => {
            val t = producer.newTransaction(NewTransactionProducerPolicy.CheckpointAsyncIfOpened, 0)
            t.send("data")
            t.checkpoint(false)
          })
        l.countDown()
      }
    })

    val t2 = new Thread(new Runnable {
      override def run(): Unit = {
        (0 until COUNT)
          .foreach(i => {
            val t = producer.newTransaction(NewTransactionProducerPolicy.CheckpointAsyncIfOpened, 1)
            t.send("data")
            t.checkpoint(false)
          })
        l.countDown()
      }
    })

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    l.await()

  }



  override def afterAll(): Unit = {
    producer.stop()
    onAfterAll()
  }
}