package agents.integration

import com.bwsw.tstreams.agents.consumer.Offset.{Newest, Oldest}
import com.bwsw.tstreams.agents.consumer.subscriber.Callback
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.env.TSF_Dictionary
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{LocalGeneratorCreator, TestUtils}

/**
  * Created by Ivan Kudryavtsev on 26.08.16.
  */
class SubscriberBasicPubSubTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
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

  it should "handle all transactions produced by producer" in {
    var subsciberTransactionsAmount = 0
    val producer = f.getProducer[String](
      name = "test_producer",
      transactionGenerator = LocalGeneratorCreator.getGen(),
      converter = stringToArrayByteConverter,
      partitions = Set(0, 1, 2),
      isLowPriority = false)

    val s = f.getSubscriber[String](name = "sv2",
      transactionGenerator = LocalGeneratorCreator.getGen(),
      converter = arrayByteToStringConverter, partitions = Set(0, 1, 2),
      offset = Oldest,
      isUseLastOffset = true,
      callback = new Callback[String] {
        override def onTransaction(consumer: TransactionOperator[String], transaction: ConsumerTransaction[String]): Unit = this.synchronized {
          subsciberTransactionsAmount += 1
          transaction.getAll()
        }
      })
    s.start()
    val TOTAL = 100
    for (it <- 0 until TOTAL) {
      val transaction = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
      transaction.send("test")
      transaction.checkpoint()
    }
    producer.stop()
    Thread.sleep(1000)
    s.stop()
    subsciberTransactionsAmount shouldBe TOTAL
  }

  it should "handle all transactions produced by two different producers" in {
    var subscriberTransactionsAmount = 0
    val producer1 = f.getProducer[String](
      name = "test_producer",
      transactionGenerator = LocalGeneratorCreator.getGen(),
      converter = stringToArrayByteConverter,
      partitions = Set(0, 1, 2),
      isLowPriority = false)

    val s = f.getSubscriber[String](name = "sv2",
      transactionGenerator = LocalGeneratorCreator.getGen(),
      converter = arrayByteToStringConverter, partitions = Set(0, 1, 2),
      offset = Newest,
      isUseLastOffset = true,
      callback = new Callback[String] {
        override def onTransaction(consumer: TransactionOperator[String], transaction: ConsumerTransaction[String]): Unit = this.synchronized {
          subscriberTransactionsAmount += 1
        }
      })
    s.start()
    val TOTAL = 100
    for (it <- 0 until TOTAL) {
      val transaction = producer1.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
      transaction.send("test")
      transaction.checkpoint()
    }
    producer1.stop()
    val producer2 = f.getProducer[String](
      name = "test_producer2",
      transactionGenerator = LocalGeneratorCreator.getGen(),
      converter = stringToArrayByteConverter,
      partitions = Set(0, 1, 2),
      isLowPriority = false)
    for (it <- 0 until TOTAL) {
      val transaction = producer2.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
      transaction.send("test")
      transaction.checkpoint()
    }
    producer2.stop()
    Thread.sleep(1000)
    s.stop()
    subscriberTransactionsAmount shouldBe TOTAL * 2
  }

  override def afterAll(): Unit = {
    onAfterAll()
  }
}
