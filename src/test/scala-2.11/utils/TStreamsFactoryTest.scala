package utils

import com.bwsw.tstreams.agents.producer.ProducerPolicies
import com.bwsw.tstreams.converter.StringToArrayByteConverter
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

  val randomKeyspace = randomString
  val cluster = Cluster.builder().addContactPoint("localhost").build()
  val session = cluster.connect()
  CassandraHelper.createKeyspace(session, randomKeyspace)
  CassandraHelper.createMetadataTables(session, randomKeyspace)
  logger.info("Begin create TStreamFactory")
  val f = new TStreamsFactory()
  f.setProperty(TSF_Dictionary.Metadata.Cluster.namespace,randomKeyspace).
    setProperty(TSF_Dictionary.Data.Cluster.namespace,"test").
    setProperty(TSF_Dictionary.Stream.name, "test-stream")
  logger.info("End create TStreamFactory")

  val p = f.getProducer[String](
    isLowPriority = false,
    txnGenerator = LocalGeneratorCreator.getGen(),
    converter = new StringToArrayByteConverter,
    partitions = List(0) )

  "UniversalFactory.getProducer" should "return producer object" in {
    p != null shouldEqual true
  }

  val txn = p.newTransaction(policy = ProducerPolicies.errorIfOpen,
                              nextPartition = 0)
  txn.send("test1")
  txn.send("test2")
  txn.checkpoint()

  p.stop()

  session.close()
  cluster.close()

}
