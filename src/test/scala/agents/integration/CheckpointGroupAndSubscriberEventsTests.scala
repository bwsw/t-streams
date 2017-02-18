package agents.integration

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.consumer.Offset.Newest
import com.bwsw.tstreams.agents.consumer.subscriber.Callback
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.agents.group.CheckpointGroup
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.env.ConfigurationOptions
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{LocalGeneratorCreator, TestUtils}

/**
  * Created by ivan on 13.09.16.
  */
class CheckpointGroupAndSubscriberEventsTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  System.setProperty("DEBUG", "true")
  System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "DEBUG")

  f.setProperty(ConfigurationOptions.Stream.NAME, "test_stream")
    .setProperty(ConfigurationOptions.Stream.PARTITIONS, 3)
    .setProperty(ConfigurationOptions.Stream.TTL, 60 * 10)
    .setProperty(ConfigurationOptions.Coordination.CONNECTION_TIMEOUT, 7)
    .setProperty(ConfigurationOptions.Coordination.TTL, 7)
    .setProperty(ConfigurationOptions.Producer.TRANSPORT_TIMEOUT, 5)
    .setProperty(ConfigurationOptions.Producer.Transaction.TTL, 6)
    .setProperty(ConfigurationOptions.Producer.Transaction.KEEP_ALIVE, 2)
    .setProperty(ConfigurationOptions.Consumer.TRANSACTION_PRELOAD, 10)
    .setProperty(ConfigurationOptions.Consumer.DATA_PRELOAD, 10)
    .setProperty(ConfigurationOptions.Consumer.Subscriber.POLLING_FREQUENCY_DELAY, 2000)

  val producer = f.getProducer[String](
    name = "test_producer",
    transactionGenerator = LocalGeneratorCreator.getGen(),
    converter = stringToArrayByteConverter,
    partitions = Set(0))

  "Group commit" should "checkpoint all AgentsGroup state" in {
    val l = new CountDownLatch(1)
    var transactionsCounter: Int = 0

    val group = new CheckpointGroup()

    group.add(producer)

    val subscriber = f.getSubscriber[String](name = "ss+2",
      transactionGenerator = LocalGeneratorCreator.getGen(),
      converter = arrayByteToStringConverter,
      partitions = Set(0),
      offset = Newest,
      useLastOffset = true,
      callback = new Callback[String] {
        override def onTransaction(consumer: TransactionOperator[String], transaction: ConsumerTransaction[String]): Unit = this.synchronized {
          transactionsCounter += 1
          if (transactionsCounter == 2) {
            l.countDown()
          }
        }
      })
    subscriber.start()
    val txn1 = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened, 0)
    txn1.send("test")
    group.checkpoint()
    val txn2 = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened, 0)
    txn2.send("test")
    group.checkpoint()
    l.await(5, TimeUnit.SECONDS) shouldBe true
    transactionsCounter shouldBe 2
    subscriber.stop()
  }

  override def afterAll(): Unit = {
    producer.stop()
    onAfterAll()
  }
}