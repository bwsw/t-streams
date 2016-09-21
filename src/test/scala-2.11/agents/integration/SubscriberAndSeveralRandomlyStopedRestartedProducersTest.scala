package agents.integration

/**
  * Created by mendelbaum_ma on 20.09.16.
  */

import java.util.UUID
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.consumer.Offset.Newest
import com.bwsw.tstreams.agents.consumer.subscriber.Callback
import com.bwsw.tstreams.agents.consumer.{Transaction, TransactionOperator}
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.env.TSF_Dictionary
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{LocalGeneratorCreator, TestUtils}
import scala.collection.mutable.ListBuffer
import scala.util.Random

class SubscriberAndSeveralRandomlyStopedRestartedProducersTest extends FlatSpec with Matchers with BeforeAndAfterAll
  with TestUtils {

  val  N = 10                         // totalPartitions
  val MAX_TO_SEND =100                // totalTxn
  val  M =10                          // producersAmount
  var FAIL_PROBABILITY: Double = 1.25 // Percentage with hundredths
  val MAX_WAIT = 25                   //   sec

  val PARTITIONS = (0 until N).toSet


  f.setProperty(TSF_Dictionary.Stream.NAME, "test_stream").
    setProperty(TSF_Dictionary.Stream.PARTITIONS, N).
    setProperty(TSF_Dictionary.Stream.TTL, 60 * 10).
    setProperty(TSF_Dictionary.Coordination.CONNECTION_TIMEOUT, 7).
    setProperty(TSF_Dictionary.Coordination.TTL, 7).
    setProperty(TSF_Dictionary.Producer.TRANSPORT_TIMEOUT, 5).
    setProperty(TSF_Dictionary.Producer.Transaction.TTL, 6).
    setProperty(TSF_Dictionary.Producer.Transaction.KEEP_ALIVE, 2).
    setProperty(TSF_Dictionary.Consumer.TRANSACTION_PRELOAD, 10).
    setProperty(TSF_Dictionary.Consumer.DATA_PRELOAD, 10)


  it should "One Subskriber and several Producers, which are randomly stopped and restarted for sending a predetermined" +
    " amount of transactions from each. Sophisticated master election. " in {

    val bs = ListBuffer[UUID]()
    val ls = new CountDownLatch(1)
    var transactionsCounter = 0

    val subscriber = f.getSubscriber[String](
      name = "Subscriber for test",
      txnGenerator = LocalGeneratorCreator.getGen(),
      converter = arrayByteToStringConverter,
      partitions = (0 until N).toSet,
      offset = Newest,
      isUseLastOffset =  false,
      callback = new Callback[String] {
        override def onEvent(consumer: TransactionOperator[String], txn: Transaction[String]): Unit = this.synchronized {
          bs.append(txn.getTransactionUUID())
          transactionsCounter += 1
        }
      })

    subscriber.start()

    var m:Int = 0
    var Mprodusers = M    //   account of producers with replacement after stop
    while ( m < Mprodusers ) {
      var i = 0

      def newProducer1() {
        var probability:Double = 99
        m += 1
        val producer1 = f.getProducer[String](
          name = "test_producer1",
          txnGenerator = LocalGeneratorCreator.getGen(),
          converter = stringToArrayByteConverter,
          partitions = PARTITIONS,
          isLowPriority = false)

        val producerThread = new Thread(new Runnable {
          def run() {
            while (i < MAX_TO_SEND) {
              val t = producer1.newTransaction(policy = NewTransactionProducerPolicy.CheckpointIfOpened)
              if (probability < FAIL_PROBABILITY) {
                producer1.stop()
                newProducer1()
                Mprodusers += 1
              }
              else {
                t.send("test")
                t.checkpoint()
                probability = Random.nextInt(9999)/100
                i += 1
                if (i == MAX_TO_SEND) producer1.stop()
              }
            }
          }
        })

        producerThread.start()
        producerThread.join(MAX_WAIT * 1000)
      }
      newProducer1()
    }
    ls.await(MAX_WAIT, TimeUnit.SECONDS)
    subscriber.stop()

    transactionsCounter shouldBe MAX_TO_SEND*M
  }
  override def afterAll(): Unit = {
    onAfterAll()
  }
}







