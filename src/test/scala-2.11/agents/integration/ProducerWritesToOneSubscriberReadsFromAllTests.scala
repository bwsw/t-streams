package agents.integration

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.consumer.{Transaction, TransactionOperator}
import com.bwsw.tstreams.agents.consumer.subscriber.Callback
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.env.TSF_Dictionary
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import testutils.{LocalGeneratorCreator, TestUtils}

/**
  * Created by ivan on 14.09.16.
  */
class ProducerWritesToOneSubscriberReadsFromAllTests   extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils  {
  f.setProperty(TSF_Dictionary.Stream.NAME,"test_stream").
    setProperty(TSF_Dictionary.Stream.PARTITIONS,3).
    setProperty(TSF_Dictionary.Stream.TTL, 60 * 10).
    setProperty(TSF_Dictionary.Coordination.CONNECTION_TIMEOUT, 7).
    setProperty(TSF_Dictionary.Coordination.TTL, 7).
    setProperty(TSF_Dictionary.Producer.TRANSPORT_TIMEOUT, 5).
    setProperty(TSF_Dictionary.Producer.Transaction.TTL, 3).
    setProperty(TSF_Dictionary.Producer.Transaction.KEEP_ALIVE, 1).
    setProperty(TSF_Dictionary.Consumer.TRANSACTION_PRELOAD, 10).
    setProperty(TSF_Dictionary.Consumer.DATA_PRELOAD, 10)

  it should "handle all transactions produced by producer" in {
    var subTxns = 0
    val producer = f.getProducer[String](
      name = "test_producer",
      txnGenerator = LocalGeneratorCreator.getGen(),
      converter = stringToArrayByteConverter,
      partitions = Set(0),
      isLowPriority = false)

    val s = f.getSubscriber[String](name = "sv2",
      txnGenerator = LocalGeneratorCreator.getGen(),
      converter = arrayByteToStringConverter, partitions = Set(0,1,2),
      offset = Oldest,
      isUseLastOffset = true,
      callback = new Callback[String] {
        override def onEvent(consumer: TransactionOperator[String], txn: Transaction[String]): Unit = this.synchronized {
          subTxns += 1
        }
      })
    s.start()
    val TOTAL = 10000
    for(it <- 0 until TOTAL) {
      val txn = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
      txn.send("test")
      txn.checkpoint()
    }
    producer.stop()
    Thread.sleep(1000)
    s.stop()
    subTxns shouldBe TOTAL
  }

  override def afterAll(): Unit = {
    onAfterAll()
  }
}

