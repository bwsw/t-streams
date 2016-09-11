package agents.integration

import java.util.UUID
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.consumer.Offset.Newest
import com.bwsw.tstreams.agents.consumer.{Transaction, TransactionOperator}
import com.bwsw.tstreams.agents.consumer.subscriber.Callback
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.converter.{ArrayByteToStringConverter, StringToArrayByteConverter}
import com.bwsw.tstreams.env.TSF_Dictionary
import com.bwsw.tstreams.generator.LocalTimeUUIDGenerator
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.TestUtils

import scala.util.Random

/**
  * Created by Ivan Kudryavtsev on 08.09.16.
  */
class SubscriberWithManyProcessingEnginesThreadsTest  extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  val TOTAL_TRANSACTIONS              = 1000
  val TOTAL_ITEMS                     = 1
  val TOTAL_PARTITIONS                = 10
  val PARTITIONS                           = (0 until TOTAL_PARTITIONS).toSet
  val PROCESSING_ENGINES_THREAD_POOL  = 10
  val TRANSACTION_BUFFER_THREAD_POOL = 10

  val POLLING_FREQUENCY_DELAY         = 1000

  f.setProperty(TSF_Dictionary.Stream.NAME,"test_stream")
    .setProperty(TSF_Dictionary.Consumer.Subscriber.PERSISTENT_QUEUE_PATH, null)
    .setProperty(TSF_Dictionary.Stream.NAME, "test-stream")
    .setProperty(TSF_Dictionary.Consumer.Subscriber.POLLING_FREQUENCY_DELAY, POLLING_FREQUENCY_DELAY)
    .setProperty(TSF_Dictionary.Consumer.Subscriber.PROCESSING_ENGINES_THREAD_POOL, PROCESSING_ENGINES_THREAD_POOL)
    .setProperty(TSF_Dictionary.Consumer.Subscriber.TRANSACTION_BUFFER_THREAD_POOL, TRANSACTION_BUFFER_THREAD_POOL)
    .setProperty(TSF_Dictionary.Stream.PARTITIONS, TOTAL_PARTITIONS)

  it should s"Start and work correctly with PROCESSING_ENGINES_THREAD_POOL=${PROCESSING_ENGINES_THREAD_POOL}" in {
    val awaitTransactionsLatch = new CountDownLatch(1)
    var transactionsCounter = 0

    val subscriber = f.getSubscriber[String](
      name          = "test_subscriber",              // name of the subscribing consumer
      txnGenerator  = new LocalTimeUUIDGenerator,     // where it can get transaction uuids
      converter     = new ArrayByteToStringConverter, // vice versa converter to string
      partitions    = PARTITIONS,                        // active partitions
      offset        = Newest,                         // it will start from newest available partitions
      isUseLastOffset = false,                        // will ignore history
      callback = new Callback[String] {
        override def onEvent(op: TransactionOperator[String], txn: Transaction[String]): Unit = this.synchronized {
          transactionsCounter += 1
          if (transactionsCounter % 100 == 0) {
            logger.info(s"I have read ${transactionsCounter} transactions up to now.")
            op.checkpoint()
          }
          if(transactionsCounter == TOTAL_TRANSACTIONS)                                              // if the producer sent all information, then end
            awaitTransactionsLatch.countDown()
        }
      })

    subscriber.start() // start subscriber to operate

    val producerThread = new Thread(new Runnable {
      override def run(): Unit = {
        // create producer
        val producer = f.getProducer[String](
          name = "test_producer",                     // name of the producer
          txnGenerator = new LocalTimeUUIDGenerator,  // where it will get new transactions
          converter = new StringToArrayByteConverter, // converter from String to internal data presentation
          partitions = PARTITIONS,                       // active partitions
          isLowPriority = false)                      // agent can be a master

        (0 until TOTAL_TRANSACTIONS).foreach(
          i => {
            val t = producer.newTransaction(policy = NewTransactionProducerPolicy.CheckpointIfOpened) // create new transaction
            (0 until TOTAL_ITEMS).foreach(j => {
              val v = Random.nextInt()
              t.send(s"${v}")
            })
            if (i % 100 == 0)
              logger.info(s"I have written ${i} transactions up to now.")
            t.checkpoint(false)  // checkpoint the transaction
          })
        producer.stop()   // stop operation
      }
    })

    producerThread.start()
    producerThread.join()
    awaitTransactionsLatch.await(POLLING_FREQUENCY_DELAY + 1000, TimeUnit.MILLISECONDS)
    subscriber.stop() // stop operation
    transactionsCounter shouldBe TOTAL_TRANSACTIONS
  }

  override def afterAll(): Unit = {
    onAfterAll()
  }

}
