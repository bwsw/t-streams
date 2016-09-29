package agents.integration

/**
  * Created by Ivan Kudryavtsev on 21.09.16.
  */

import java.util.concurrent.CountDownLatch

import com.bwsw.tstreams.agents.consumer.Offset.Newest
import com.bwsw.tstreams.agents.consumer.subscriber.Callback
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.agents.producer.{NewTransactionProducerPolicy, Producer}
import com.bwsw.tstreams.converter.StringToArrayByteConverter
import com.bwsw.tstreams.env.{TSF_Dictionary, TStreamsFactory}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{LocalGeneratorCreator, TestUtils}

import scala.util.Random

class ProducerMasterChangeComplexTest  extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

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
        partitions = partitions,
        isLowPriority = false)
    }
  }
  val PRODUCERS_AMOUNT          = 10
  val TRANSACTIONS_AMOUNT_EACH  = 100
  val PROBABILITY               = 0.005 // 0.01=1%
  val PARTITIONS                = (0 until 5).toSet
  val MAX_WAIT_AFTER_ALL_PRODUCERS = 5

  val onCompleteLatch   = new CountDownLatch(PRODUCERS_AMOUNT)
  val waitCompleteLatch = new CountDownLatch(1)

  f.setProperty(TSF_Dictionary.Stream.NAME, "test_stream").
    setProperty(TSF_Dictionary.Stream.PARTITIONS, 5).
    setProperty(TSF_Dictionary.Stream.TTL, 60 * 10).
    setProperty(TSF_Dictionary.Coordination.CONNECTION_TIMEOUT, 7).
    setProperty(TSF_Dictionary.Coordination.TTL, 7).
    setProperty(TSF_Dictionary.Producer.TRANSPORT_TIMEOUT, 5).
    setProperty(TSF_Dictionary.Producer.Transaction.TTL, 3).
    setProperty(TSF_Dictionary.Producer.Transaction.KEEP_ALIVE, 1).
    setProperty(TSF_Dictionary.Consumer.TRANSACTION_PRELOAD, 10).
    setProperty(TSF_Dictionary.Consumer.DATA_PRELOAD, 10)

  var subscriberCounter = 0
  val subscriber = f.getSubscriber[String](name = "s",
    transactionGenerator = LocalGeneratorCreator.getGen(),
    converter = arrayByteToStringConverter,
    partitions = PARTITIONS,     // Set(0),
    offset = Newest,
    isUseLastOffset = false, // true
    callback = new Callback[String] {
      override def onTransaction(consumer: TransactionOperator[String], transaction: ConsumerTransaction[String]): Unit = this.synchronized {
        subscriberCounter += 1
        if(subscriberCounter == PRODUCERS_AMOUNT * TRANSACTIONS_AMOUNT_EACH)
          waitCompleteLatch.countDown()
      }
    })

//  it should "handle multiple master change correctly" in {
//
//    subscriber.start()
//
//    val producersThreads = (0 until PRODUCERS_AMOUNT)
//      .map(producer => new ProducerWorker(f, onCompleteLatch, TRANSACTIONS_AMOUNT_EACH, PROBABILITY).run(PARTITIONS))
//
//    onCompleteLatch.await()
//    producersThreads.foreach(thread => thread.join())
//    waitCompleteLatch.await(MAX_WAIT_AFTER_ALL_PRODUCERS, TimeUnit.SECONDS)
//    subscriber.stop()
//
//    subscriberCounter shouldBe TRANSACTIONS_AMOUNT_EACH * PRODUCERS_AMOUNT
//  }

  override def afterAll() {
    onAfterAll()
  }
}







