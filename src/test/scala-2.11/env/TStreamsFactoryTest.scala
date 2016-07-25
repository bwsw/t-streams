package env

import com.bwsw.tstreams.agents.consumer.Offsets.Oldest
import com.bwsw.tstreams.agents.producer.ProducerPolicies
import com.bwsw.tstreams.converter.{ArrayByteToStringConverter, StringToArrayByteConverter}
import com.bwsw.tstreams.env.{TSF_Dictionary, TStreamsFactory}
import com.datastax.driver.core.Cluster
import org.slf4j.LoggerFactory

/**
  * Created by ivan on 23.07.16.
  */
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._


class TStreamsFactoryTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  private val logger = LoggerFactory.getLogger(this.getClass)

  val randomKeyspace = createRandomKeyspace

  val f = new TStreamsFactory()
  f.setProperty(TSF_Dictionary.Metadata.Cluster.namespace,randomKeyspace).
    setProperty(TSF_Dictionary.Data.Cluster.namespace,"test").
    setProperty(TSF_Dictionary.Stream.name, "test-stream")

  "UniversalFactory.getProducer" should "return producer object" in {
    val p = f.getProducer[String](
      name          = "test-producer-1",
      isLowPriority = false,
      txnGenerator  = LocalGeneratorCreator.getGen(),
      converter     = new StringToArrayByteConverter,
      partitions    = List(0) )

    p != null shouldEqual true

    val txn = p.newTransaction(
      policy        = ProducerPolicies.errorIfOpened,
      nextPartition = 0)

    txn.send("test1")
    txn.send("test2")
    txn.checkpoint()

    p.stop()

  }

  "UniversalFactory.getConsumer" should "return consumer object" in {
    val c = f.getConsumer[String](
      name          = "test-consumer-1",
      txnGenerator  = LocalGeneratorCreator.getGen(),
      converter     = new ArrayByteToStringConverter,
      partitions    = List(0),
      offset        = Oldest)

    c != null shouldEqual true

    val txn = c.getTransaction.get
    val data = txn.getAll()

    data.size shouldBe 2
    c.checkpoint()
  }

  override def afterAll(): Unit = {
    f.close()
    super.afterAll()
  }

}
