package metadata

import java.net.InetSocketAddress

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.TestUtils


class MetadataStorageFactoryTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  "MetadataStorageFactory.getInstance()" should "return MetadataStorage instance" in {
    val factory = new MetadataStorageFactory
    val instance = factory.getInstance(CassandraConnectorConf(Set(new InetSocketAddress("localhost", 9142))), randomKeyspace)
    val checkVal = instance.isInstanceOf[MetadataStorage]
    checkVal shouldEqual true
  }

  override def afterAll(): Unit = {
    onAfterAll()
  }
}
