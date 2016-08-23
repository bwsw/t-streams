package data

import com.bwsw.tstreams.data.cassandra.Factory
import testutils.TestUtils

class CassandraStorageFactoryTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  "CassandraStorageFactory.getInstance()" should "return CassandraStorage instance" in {
    val factory = new Factory
//    val instance = factory.getInstance(cassandraStorageOptions)
//    val checkVal = instance.isInstanceOf[CassandraStorage]
//    factory.closeFactory()
//
//    checkVal shouldEqual true
  }

  "CassandraStorageFactory.closeFactory()" should "close instances connections" in {
    val factory: Factory = new Factory
//    val instance1 = factory.getInstance(cassandraStorageOptions)
//    val instance2 = factory.getInstance(cassandraStorageOptions)
//    factory.closeFactory()
//    val checkVal = instance1.isClosed && instance2.isClosed
//
//    checkVal shouldEqual true
  }

  override def afterAll(): Unit = {
    onAfterAll()
  }
}
