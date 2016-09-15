package agents.integration

import java.util.concurrent.CountDownLatch

import com.bwsw.tstreams.agents.consumer.Offset.Newest
import com.bwsw.tstreams.agents.consumer.subscriber.Callback
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.agents.group.CheckpointGroup
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.env.TSF_Dictionary
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{LocalGeneratorCreator, TestUtils}

/**
  * Created by ivan on 13.09.16.
  */
class CheckpointGroupAndSubscriberEventsTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  System.setProperty("DEBUG", "true")
  System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "DEBUG")

  f.setProperty(TSF_Dictionary.Stream.NAME, "test_stream")
    .setProperty(TSF_Dictionary.Stream.PARTITIONS, 3)
    .setProperty(TSF_Dictionary.Stream.TTL, 60 * 10)
    .setProperty(TSF_Dictionary.Coordination.CONNECTION_TIMEOUT, 7)
    .setProperty(TSF_Dictionary.Coordination.TTL, 7)
    .setProperty(TSF_Dictionary.Producer.TRANSPORT_TIMEOUT, 5)
    .setProperty(TSF_Dictionary.Producer.Transaction.TTL, 6)
    .setProperty(TSF_Dictionary.Producer.Transaction.KEEP_ALIVE, 2)
    .setProperty(TSF_Dictionary.Consumer.TRANSACTION_PRELOAD, 10)
    .setProperty(TSF_Dictionary.Consumer.DATA_PRELOAD, 10)
    .setProperty(TSF_Dictionary.Consumer.Subscriber.POLLING_FREQUENCY_DELAY, 2000)

  val producer = f.getProducer[String](
    name = "test_producer",
    transactionGenerator = LocalGeneratorCreator.getGen(),
    converter = stringToArrayByteConverter,
    partitions = Set(0),
    isLowPriority = false)

  "Group commit" should "checkpoint all AgentsGroup state" in {
    val l = new CountDownLatch(1)
    var ctr: Int = 0
    val group = new CheckpointGroup()

    group.add(producer)

    val s = f.getSubscriber[String](name = "ss+2",
      transactionGenerator = LocalGeneratorCreator.getGen(),
      converter = arrayByteToStringConverter,
      partitions = Set(0),
      offset = Newest,
      isUseLastOffset = true,
      callback = new Callback[String] {
        override def onTransaction(consumer: TransactionOperator[String], transaction: ConsumerTransaction[String]): Unit = this.synchronized {
          ctr += 1
          if (ctr == 2) {
            l.countDown()
          }
        }
      })
    s.start()

    val start = System.currentTimeMillis()

    producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened, 0)
    group.checkpoint()

    producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened, 0)
    group.checkpoint()

    l.await()

    val end = System.currentTimeMillis()

    logger.info(s"End - start = ${end - start}")
    end - start < 2000 shouldBe true

    s.stop()

  }

  override def afterAll(): Unit = {
    producer.stop()
    onAfterAll()
  }
}