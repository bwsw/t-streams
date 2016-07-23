package utils

import com.bwsw.tstreams.converter.StringToArrayByteConverter
import com.bwsw.tstreams.env.{TSF_Dictionary, TStreamsFactory}
import com.datastax.driver.core.Cluster

/**
  * Created by ivan on 23.07.16.
  */
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._


class TStreamsFactoryTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  val randomKeyspace = randomString
  val cluster = Cluster.builder().addContactPoint("localhost").build()
  val session = cluster.connect()
  CassandraHelper.createKeyspace(session, randomKeyspace)
  CassandraHelper.createMetadataTables(session, randomKeyspace)
  session.close()
  cluster.close()

  val f = new TStreamsFactory()
  f.setProperty(TSF_Dictionary.Metadata.Cluster.namespace,randomKeyspace).
    setProperty(TSF_Dictionary.Data.Cluster.namespace,"test").
    setProperty(TSF_Dictionary.Stream.name, "test-stream")


  val p = f.getProducer[String](
    isLowPriority = false,
    txnGenerator = LocalGeneratorCreator.getGen(),
    converter = new StringToArrayByteConverter, partitions = List(0) )

  "UniversalFactory.getProducer" should "return producer object" in {
    p != null shouldEqual true
  }
}
