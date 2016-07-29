package data

import java.net.InetSocketAddress

import com.bwsw.tstreams.data.cassandra.{CassandraStorageOptions, CassandraStorageFactory, CassandraStorage}
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import testutils.TestUtils

class CassandraStorageFactoryTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  "CassandraStorageFactory.getInstance()" should "return CassandraStorage instance" in {
    val factory = new CassandraStorageFactory
    val instance = factory.getInstance(cassandraStorageOptions)
    val checkVal = instance.isInstanceOf[CassandraStorage]
    factory.closeFactory()

    checkVal shouldEqual true
  }

  "CassandraStorageFactory.closeFactory()" should "close instances connections" in {
    val factory: CassandraStorageFactory = new CassandraStorageFactory
    val instance1 = factory.getInstance(cassandraStorageOptions)
    val instance2 = factory.getInstance(cassandraStorageOptions)
    factory.closeFactory()
    val checkVal = instance1.isClosed && instance2.isClosed

    checkVal shouldEqual true
  }

  override def afterAll(): Unit = {
      onAfterAll()
  }
}
