package agents.integration

/**
  * Created by Ivan Kudryavtsev on 21.09.16.
  */

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.consumer.Offset.Newest
import com.bwsw.tstreams.agents.consumer.subscriber.Callback
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.agents.producer.{NewTransactionProducerPolicy, Producer}
import com.bwsw.tstreams.converter.StringToArrayByteConverter
import com.bwsw.tstreams.env.{ConfigurationOptions, TStreamsFactory}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{LocalGeneratorCreator, TestUtils}

import scala.collection.mutable.ListBuffer
import scala.util.Random

class ProducerMasterChangeComplexTest  extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  val producerBuffer = ListBuffer[Long]()
  val subscriberBuffer = ListBuffer[Long]()

  class ProducerWorker(val factory: TStreamsFactory, val onCompleteLatch: CountDownLatch, val amount: Int, val probability: Double) {
    var producer: Producer[String] = null
    var counter: Int = 0
    // public because will be called
    def loop(partitions: Set[Int], checkpointModeSync: Boolean = true) = {
      while(counter < amount) {
        producer = makeNewProducer(partitions)

        while(probability < Random.nextDouble() && counter < amount) {
          val t = producer.newTransaction(policy = NewTransactionProducerPolicy.CheckpointIfOpened)
          t.send("test")
          t.checkpoint(checkpointModeSync)
          producerBuffer.synchronized {
            producerBuffer.append(t.getTransactionID())
          }
          counter += 1
        }
        producer.stop()
      }
      onCompleteLatch.countDown()
    }

    def run(partitions: Set[Int], checkpointModeSync: Boolean = true): Thread = {
      val thread = new Thread(new Runnable {
        override def run(): Unit = loop(partitions, checkpointModeSync)
      })
      thread.start()
      thread
    }

    // private - will not be called outside
    private def makeNewProducer(partitions: Set[Int]) = {
      factory.getProducer[String](
        name = "test_producer1",
        transactionGenerator = LocalGeneratorCreator.getGen(),
        converter = new StringToArrayByteConverter,
        partitions = partitions)
    }
  }
  val PRODUCERS_AMOUNT          = 10
  val TRANSACTIONS_AMOUNT_EACH  = 100
  val PROBABILITY               = 0.01 // 0.01=1%
  val PARTITIONS_COUNT          = 10
  val PARTITIONS                = (0 until PARTITIONS_COUNT).toSet
  val MAX_WAIT_AFTER_ALL_PRODUCERS = 5

  val onCompleteLatch   = new CountDownLatch(PRODUCERS_AMOUNT)
  val waitCompleteLatch = new CountDownLatch(1)

  f.setProperty(ConfigurationOptions.Stream.name, "test_stream").
    setProperty(ConfigurationOptions.Stream.partitionsCount, PARTITIONS_COUNT).
    setProperty(ConfigurationOptions.Stream.ttlSec, 60 * 10).
    setProperty(ConfigurationOptions.Coordination.connectionTimeoutMs, 7).
    setProperty(ConfigurationOptions.Coordination.sessionTimeoutMs, 7).
    setProperty(ConfigurationOptions.Producer.transportTimeoutMs, 5).
    setProperty(ConfigurationOptions.Producer.Transaction.ttlMs, 3).
    setProperty(ConfigurationOptions.Producer.Transaction.keepAliveMs, 1).
    setProperty(ConfigurationOptions.Consumer.TRANSACTION_PRELOAD, 10).
    setProperty(ConfigurationOptions.Consumer.DATA_PRELOAD, 10)

  var subscriberCounter = 0
  val subscriber = f.getSubscriber[String](name = "s",
    transactionGenerator = LocalGeneratorCreator.getGen(),
    converter = arrayByteToStringConverter,
    partitions = PARTITIONS,     // Set(0),
    offset = Newest,
    useLastOffset = false, // true
    callback = new Callback[String] {
      override def onTransaction(consumer: TransactionOperator[String], transaction: ConsumerTransaction[String]): Unit = this.synchronized {
        subscriberCounter += 1
        subscriberBuffer.synchronized {
          subscriberBuffer.append(transaction.getTransactionID())
        }
        if(subscriberCounter == PRODUCERS_AMOUNT * TRANSACTIONS_AMOUNT_EACH)
          waitCompleteLatch.countDown()
      }
    })

  it should "handle multiple master change correctly" in {

    subscriber.start()

    val producersThreads = (0 until PRODUCERS_AMOUNT)
      .map(producer => new ProducerWorker(f, onCompleteLatch, TRANSACTIONS_AMOUNT_EACH, PROBABILITY).run(PARTITIONS))

    onCompleteLatch.await()
    producersThreads.foreach(thread => thread.join())
    waitCompleteLatch.await(MAX_WAIT_AFTER_ALL_PRODUCERS, TimeUnit.SECONDS)
    subscriber.stop()

    subscriberCounter shouldBe TRANSACTIONS_AMOUNT_EACH * PRODUCERS_AMOUNT

    val intersectionSize = producerBuffer.toSet.intersect(subscriberBuffer.toSet).size

    intersectionSize shouldBe producerBuffer.size
    intersectionSize shouldBe subscriberBuffer.size

  }

  override def afterAll() {
    onAfterAll()
  }
}







