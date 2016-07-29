package metadata

import java.net.InetSocketAddress

import com.bwsw.tstreams.metadata.{MetadataStorage, MetadataStorageFactory}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.TestUtils


class MetadataStorageFactoryTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  "MetadataStorageFactory.getInstance()" should "return MetadataStorage instance" in {
    val factory = new MetadataStorageFactory
    val instance = factory.getInstance(List(new InetSocketAddress("localhost", 9042)), randomKeyspace)
    val checkVal = instance.isInstanceOf[MetadataStorage]
    checkVal shouldEqual true
  }

  override def afterAll(): Unit = {
    onAfterAll()
  }
}
