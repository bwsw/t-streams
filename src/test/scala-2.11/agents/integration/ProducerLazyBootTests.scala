package agents.integration

import com.bwsw.tstreams.agents.producer.{NewTransactionProducerPolicy, Transaction}
import com.bwsw.tstreams.env.TSF_Dictionary
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{LocalGeneratorCreator, TestUtils}

/**
  * Created by ivan on 12.09.16.
  */
class ProducerLazyBootTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {


  // keep it greater than 3
  val ALL_PARTITIONS = 2

  f.setProperty(TSF_Dictionary.Stream.NAME,"test_stream")
    .setProperty(TSF_Dictionary.Stream.PARTITIONS, ALL_PARTITIONS)
    .setProperty(TSF_Dictionary.Stream.TTL, 60 * 10)
    .setProperty(TSF_Dictionary.Coordination.CONNECTION_TIMEOUT, 7)
    .setProperty(TSF_Dictionary.Coordination.TTL, 7)
    .setProperty(TSF_Dictionary.Producer.TRANSPORT_TIMEOUT, 5)
    .setProperty(TSF_Dictionary.Producer.Transaction.TTL, 6)
    .setProperty(TSF_Dictionary.Producer.Transaction.KEEP_ALIVE, 2)
    .setProperty(TSF_Dictionary.Consumer.TRANSACTION_PRELOAD, 10)
    .setProperty(TSF_Dictionary.Consumer.DATA_PRELOAD, 10)
    .setProperty(TSF_Dictionary.Producer.MASTER_BOOTSTRAP_MODE, TSF_Dictionary.Producer.Consts.MASTER_BOOTSTRAP_MODE_LAZY)
    .setProperty(TSF_Dictionary.Coordination.PARTITION_REDISTRIBUTION_DELAY, 5000)


  it should "distribute partitions in a right way with lazy mode" in {
    val producer1 = f.getProducer[String](
      name = "test_producer",
      txnGenerator = LocalGeneratorCreator.getGen(),
      converter = stringToArrayByteConverter,
      partitions = (0 until ALL_PARTITIONS).toSet,
      isLowPriority = false)

    f.setProperty(TSF_Dictionary.Producer.MASTER_BOOTSTRAP_MODE, TSF_Dictionary.Producer.Consts.MASTER_BOOTSTRAP_MODE_FULL)
      .setProperty(TSF_Dictionary.Coordination.PARTITION_REDISTRIBUTION_DELAY, 1000)

    val producer2 = f.getProducer[String](
      name = "test_producer",
      txnGenerator = LocalGeneratorCreator.getGen(),
      converter = stringToArrayByteConverter,
      partitions = (0 until ALL_PARTITIONS).toSet,
      isLowPriority = false)

    producer1.isMeAMasterOfPartition(0) shouldBe false
    producer1.isMeAMasterOfPartition(1) shouldBe false
    producer2.isMeAMasterOfPartition(0) shouldBe true
    producer2.isMeAMasterOfPartition(1) shouldBe true

    producer2.stop()

    producer1.newTransaction(NewTransactionProducerPolicy.CheckpointIfOpened, 0)
    producer1.isMeAMasterOfPartition(0) shouldBe true
    producer1.newTransaction(NewTransactionProducerPolicy.CheckpointIfOpened, 1)
    producer1.isMeAMasterOfPartition(1) shouldBe true

    producer1.stop()

  }

  it should "become a master if lazy+vote" in {
    f.setProperty(TSF_Dictionary.Producer.MASTER_BOOTSTRAP_MODE, TSF_Dictionary.Producer.Consts.MASTER_BOOTSTRAP_MODE_LAZY_VOTE)
      .setProperty(TSF_Dictionary.Coordination.PARTITION_REDISTRIBUTION_DELAY, 1000)

    val producer = f.getProducer[String](
      name = "test_producer",
      txnGenerator = LocalGeneratorCreator.getGen(),
      converter = stringToArrayByteConverter,
      partitions = (0 until ALL_PARTITIONS).toSet,
      isLowPriority = false)

    Thread.sleep(3000)

    producer.isMeAMasterOfPartition(0) shouldBe true
    producer.isMeAMasterOfPartition(1) shouldBe true

    producer.stop()

  }

  it should "not become a master if lazy" in {
    f.setProperty(TSF_Dictionary.Producer.MASTER_BOOTSTRAP_MODE, TSF_Dictionary.Producer.Consts.MASTER_BOOTSTRAP_MODE_LAZY)
      .setProperty(TSF_Dictionary.Coordination.PARTITION_REDISTRIBUTION_DELAY, 1000)

    val producer = f.getProducer[String](
      name = "test_producer",
      txnGenerator = LocalGeneratorCreator.getGen(),
      converter = stringToArrayByteConverter,
      partitions = (0 until ALL_PARTITIONS).toSet,
      isLowPriority = false)

    Thread.sleep(3000)

    producer.isMeAMasterOfPartition(0) shouldBe false
    producer.isMeAMasterOfPartition(1) shouldBe false

    producer.stop()

  }


  it should "become a master if lazy and does transaction" in {
    f.setProperty(TSF_Dictionary.Producer.MASTER_BOOTSTRAP_MODE, TSF_Dictionary.Producer.Consts.MASTER_BOOTSTRAP_MODE_LAZY)
      .setProperty(TSF_Dictionary.Coordination.PARTITION_REDISTRIBUTION_DELAY, 1000)

    val producer = f.getProducer[String](
      name = "test_producer",
      txnGenerator = LocalGeneratorCreator.getGen(),
      converter = stringToArrayByteConverter,
      partitions = (0 until ALL_PARTITIONS).toSet,
      isLowPriority = false)

    producer.newTransaction(NewTransactionProducerPolicy.CheckpointIfOpened, 0)
    producer.isMeAMasterOfPartition(0) shouldBe true
    producer.newTransaction(NewTransactionProducerPolicy.CheckpointIfOpened, 1)
    producer.isMeAMasterOfPartition(1) shouldBe true

    producer.stop()

  }

  override def afterAll(): Unit = {
    onAfterAll()
  }
}
