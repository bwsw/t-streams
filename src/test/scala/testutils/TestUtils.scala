package testutils

import java.io.File
import java.lang.management.ManagementFactory
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import com.bwsw.tstreams.converter.{ArrayByteToStringConverter, StringToArrayByteConverter}
import com.bwsw.tstreams.debug.GlobalHooks
import com.bwsw.tstreams.env.{TSF_Dictionary, TStreamsFactory}
import com.google.common.io.Files
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.server.quorum.QuorumPeerConfig
import org.apache.zookeeper.server.{ServerConfig, ZooKeeperServerMain}
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

  val zookeeperPort = TestUtils.zookeperPort


  val logger = LoggerFactory.getLogger(this.getClass)
  val uptime = ManagementFactory.getRuntimeMXBean.getStartTime

  logger.info("-------------------------------------------------------")
  logger.info("Test suite " + this.getClass.toString + " started")
  logger.info("Test Suite uptime is " + ((System.currentTimeMillis - uptime) / 1000L).toString + " seconds")
  logger.info("-------------------------------------------------------")


  val f = new TStreamsFactory()
  f.setProperty(TSF_Dictionary.Coordination.ROOT, coordinationRoot)
    .setProperty(TSF_Dictionary.Coordination.ENDPOINTS, s"localhost:$zookeeperPort")
    .setProperty(TSF_Dictionary.Stream.NAME, "test-stream")

  //converters to convert usertype->storagetype; storagetype->usertype
  val arrayByteToStringConverter = new ArrayByteToStringConverter
  val stringToArrayByteConverter = new StringToArrayByteConverter

  val curatorClient = CuratorFrameworkFactory.builder()
    .namespace("")
    .connectionTimeoutMs(7000)
    .sessionTimeoutMs(7000)
    .retryPolicy(new ExponentialBackoffRetry(1000, 3))
    .connectString(s"127.0.0.1:$zookeeperPort").build()
  curatorClient.start()

  removeZkMetadata(f.getProperty(TSF_Dictionary.Coordination.ROOT).toString)


  /**
    * Sorting checker
    */
  def isSorted(list: ListBuffer[Long]): Boolean = {
    if (list.isEmpty)
      return true
    var checkVal = true
    var curVal = list.head
    list foreach { el =>
      if (el < curVal)
        checkVal = false
      if (el > curVal)
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
    if (curatorClient.checkExists.forPath(path) != null)
      curatorClient.delete.deletingChildrenIfNeeded().forPath(path)
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
    curatorClient.close()
  }
}

object TestUtils {

  val zookeperPort = 21810
  val cassandraPort = 9142

  private val id: AtomicInteger = new AtomicInteger(0)

  System.getProperty("java.io.tmpdir", "./target/")
  private val zk = new ZooKeeperLocal(zookeperPort, Files.createTempDir().toString)

  def moveId(): Int = {
    val rid = id.incrementAndGet()
    rid
  }

  def getKeyspace(id: Int): String = "tk_" + id.toString

  def getCoordinationRoot(id: Int): String = "/" + getKeyspace(id)

}

class ZooKeeperLocal(zookeperPort: Int, tmp: String) {

  val properties = new Properties()
  properties.setProperty("tickTime", "2000")
  properties.setProperty("initLimit", "10")
  properties.setProperty("syncLimit", "5")
  properties.setProperty("dataDir", s"$tmp")
  properties.setProperty("clientPort", s"$zookeperPort")

  val zooKeeperServer = new ZooKeeperServerMain
  val quorumConfiguration = new QuorumPeerConfig()
  quorumConfiguration.parseProperties(properties)

  val configuration = new ServerConfig()

  configuration.readFrom(quorumConfiguration)

  new Thread() {
    override def run() = {
      zooKeeperServer.runFromConfig(configuration)
    }
  }.start()
}
