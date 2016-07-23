package utils

import com.bwsw.tstreams.converter.StringToArrayByteConverter
import com.bwsw.tstreams.utils.{UF_Dictionary, UniversalFactory}
import com.datastax.driver.core.Cluster

/**
  * Created by ivan on 23.07.16.
  */
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._


class UniversalFactoryTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  "UniversalFactory.getProducer" should "return producer object" in {

    val randomKeyspace = randomString
    val cluster = Cluster.builder().addContactPoint("localhost").build()
    val session = cluster.connect()
    CassandraHelper.createKeyspace(session, randomKeyspace)
    CassandraHelper.createMetadataTables(session, randomKeyspace)
    session.close()
    cluster.close()

    val f = new UniversalFactory()
    f.setProperty(UF_Dictionary.Metadata.Cluster.namespace,randomKeyspace).
      setProperty(UF_Dictionary.Data.Cluster.namespace,"test").
      setProperty(UF_Dictionary.Stream.name, "test-stream")


    val p = f.getProducer[String](
        isLowPriority = false,
        txnGenerator = LocalGeneratorCreator.getGen(),
        converter = new StringToArrayByteConverter, partitions = List(0) )
    p != null shouldEqual true
   }
}
