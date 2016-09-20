package agents.integration

import java.util.concurrent.CountDownLatch

import com.bwsw.tstreams.env.TSF_Dictionary
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{LocalGeneratorCreator, TestUtils}

import scala.collection.mutable.ListBuffer

/**
  * Created by Ivan Kudryavtsev on 20.09.16.
  */
class ProducerMasterDistributionValidityTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  val COUNT = 5
  val PARTS = (0 until COUNT * 2).toSet

  f.setProperty(TSF_Dictionary.Stream.NAME, "test_stream").
    setProperty(TSF_Dictionary.Stream.PARTITIONS, COUNT * 2).
    setProperty(TSF_Dictionary.Stream.TTL, 60 * 10).
    setProperty(TSF_Dictionary.Coordination.CONNECTION_TIMEOUT, 7).
    setProperty(TSF_Dictionary.Coordination.TTL, 7).
    setProperty(TSF_Dictionary.Producer.TRANSPORT_TIMEOUT, 5).
    setProperty(TSF_Dictionary.Producer.Transaction.TTL, 3).
    setProperty(TSF_Dictionary.Producer.Transaction.KEEP_ALIVE, 1).
    setProperty(TSF_Dictionary.Consumer.TRANSACTION_PRELOAD, 10).
    setProperty(TSF_Dictionary.Consumer.DATA_PRELOAD, 10)

  it should "switching the master after 100 transactions " in {

    val bp = ListBuffer[Long]()
    val bs = ListBuffer[Long]()

    val lp2 = new CountDownLatch(1)
    val ls = new CountDownLatch(1)

    val producer1 = f.getProducer[String](
      name = "test_producer1",
      transactionGenerator = LocalGeneratorCreator.getGen(),
      converter = stringToArrayByteConverter,
      partitions = PARTS,
      isLowPriority = false)

    producer1.awaitPartitionRedistributionThreadComplete()

    val producer2 = f.getProducer[String](
      name = "test_producer2",
      transactionGenerator = LocalGeneratorCreator.getGen(),
      converter = stringToArrayByteConverter,
      partitions = PARTS,
      isLowPriority = false)

    producer2.awaitPartitionRedistributionThreadComplete()

    PARTS.count(p => producer1.isLocalMePartitionMaster(p)) shouldBe COUNT
    PARTS.count(p => producer2.isLocalMePartitionMaster(p)) shouldBe COUNT

    producer1.dumpPartitionsOwnership()
    producer2.dumpPartitionsOwnership()

    producer1.stop()
    producer2.stop()

  }

  override def afterAll(): Unit = {
    onAfterAll()
  }
}

