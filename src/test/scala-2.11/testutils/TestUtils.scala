package testutils

import java.io.File
import java.lang.management.ManagementFactory
import java.net.InetSocketAddress
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import com.aerospike.client.Host
import com.bwsw.tstreams.common.{ZookeeperDLMService, CassandraConnectionPool, CassandraHelper}
import com.bwsw.tstreams.converter.{ArrayByteToStringConverter, StringToArrayByteConverter}
import com.bwsw.tstreams.data.aerospike.{AerospikeStorageFactory, AerospikeStorageOptions}
import com.bwsw.tstreams.data.cassandra.{CassandraStorageFactory, CassandraStorageOptions}
import com.bwsw.tstreams.debug.GlobalHooks
import com.bwsw.tstreams.env.{TSF_Dictionary, TStreamsFactory}
import com.bwsw.tstreams.metadata.MetadataStorageFactory
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

/**
  * Test help utils
  */
trait TestUtils {
  protected val batchSizeTestVal = 5

  /**
    * Random alpha string generator
    *
    * @return Alpha string
    */
  val id = TestUtils.moveId()
  val randomString = TestUtils.getKeyspace(id)
  val coordinationRoot = TestUtils.getCoordinationRoot(id)

  val logger = LoggerFactory.getLogger(this.getClass)
  val uptime = ManagementFactory.getRuntimeMXBean.getStartTime

  logger.info("-------------------------------------------------------")
  logger.info("Test suite " + this.getClass.toString + " started")
  logger.info("Test Suite uptime is " + ((System.currentTimeMillis - uptime)/1000L).toString + " seconds")
  logger.info("-------------------------------------------------------")


  val cluster = CassandraConnectionPool.getCluster(List(new InetSocketAddress("localhost", 9042)))
  val session = CassandraConnectionPool.getSession(List(new InetSocketAddress("localhost", 9042)), null)

  def createRandomKeyspace(): String = {
    val randomKeyspace = randomString
    CassandraHelper.createKeyspace(session, randomKeyspace)
    CassandraHelper.createMetadataTables(session, randomKeyspace)
    CassandraHelper.createDataTable(session, randomKeyspace)
    CassandraHelper.clearMetadataTables(session, randomKeyspace)
    CassandraHelper.clearDataTable(session, randomKeyspace)

    randomKeyspace
  }

  val randomKeyspace = createRandomKeyspace()

  val f = new TStreamsFactory()
  f.setProperty(TSF_Dictionary.Metadata.Cluster.NAMESPACE, randomKeyspace).
    setProperty(TSF_Dictionary.Data.Cluster.NAMESPACE, "test").
    setProperty(TSF_Dictionary.Coordination.ROOT, coordinationRoot).
    setProperty(TSF_Dictionary.Producer.BIND_HOST, TestUtils.getPort).
    setProperty(TSF_Dictionary.Consumer.Subscriber.BIND_PORT, TestUtils.getPort).
    setProperty(TSF_Dictionary.Consumer.Subscriber.PERSISTENT_QUEUE_PATH, "/tmp/" + randomKeyspace).
    setProperty(TSF_Dictionary.Stream.NAME, "test-stream")

  //metadata/data factories
  val metadataStorageFactory = new MetadataStorageFactory
  val storageFactory = new AerospikeStorageFactory
  val cassandraStorageFactory = new CassandraStorageFactory
  val cassandraStorageOptions = new CassandraStorageOptions(List(new InetSocketAddress("localhost", 9042)), randomKeyspace)


  //converters to convert usertype->storagetype; storagetype->usertype
  val arrayByteToStringConverter = new ArrayByteToStringConverter
  val stringToArrayByteConverter = new StringToArrayByteConverter

  //aerospike storage options
  val hosts = List(
    new Host("localhost", 3000))

  val aerospikeOptions = new AerospikeStorageOptions("test", hosts)
  val zkService = new ZookeeperDLMService("", List(new InetSocketAddress("localhost", 2181)), 7, 7)

  removeZkMetadata(f.getProperty(TSF_Dictionary.Coordination.ROOT).toString)


  /**
    * Sorting checker
    */
  def isSorted(list: ListBuffer[UUID]): Boolean = {
    if (list.isEmpty)
      return true
    var checkVal = true
    var curVal = list.head
    list foreach { el =>
      if (el.timestamp() < curVal.timestamp())
        checkVal = false
      if (el.timestamp() > curVal.timestamp())
        curVal = el
    }
    checkVal
  }

  /**
    * Remove zk metadata from concrete root
    *
    * @param path Zk root to delete
    */
  def removeZkMetadata(path: String) = {
    if (zkService.exist(path))
      zkService.deleteRecursive(path)
  }

  /**
    * Remove directory recursive
    *
    * @param f Dir to remove
    */
  def remove(f: File): Unit = {
    if (f.isDirectory) {
      for (c <- f.listFiles())
        remove(c)
    }
    f.delete()
  }

  def onAfterAll() = {
    System.setProperty("DEBUG", "false")
    GlobalHooks.addHook(GlobalHooks.preCommitFailure, () => ())
    GlobalHooks.addHook(GlobalHooks.afterCommitFailure, () => ())
    removeZkMetadata(f.getProperty(TSF_Dictionary.Coordination.ROOT).toString)
    removeZkMetadata("/unit")
    metadataStorageFactory.closeFactory()
    storageFactory.closeFactory()
    cassandraStorageFactory.closeFactory()
  }
}

object TestUtils {

  private val id: AtomicInteger = new AtomicInteger(0)
  private val port = new AtomicInteger(8000)

  def moveId(): Int = {
    val rid = id.incrementAndGet()
    rid
  }

  def getKeyspace(id: Int): String = "tk_" + id.toString

  def getCoordinationRoot(id: Int): String = "/" + getKeyspace(id)


  def getPort(): Int = {
    val rport = port.incrementAndGet()
    rport
  }

}