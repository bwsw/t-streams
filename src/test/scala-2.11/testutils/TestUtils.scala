package testutils

import java.io.File
import java.net.InetSocketAddress
import java.util.UUID

import com.bwsw.tstreams.common.zkservice.ZkService
import com.datastax.driver.core.Cluster

import scala.collection.mutable.ListBuffer

/**
 * Test help utils
 */
trait TestUtils {
  protected val batchSizeTestVal = 5

  /**
   * Random alpha string generator
   * @return Alpha string
   */
  def randomString =
    RandomStringCreator.randomAlphaString(10)

  def createRandomKeyspace(): String = {
    val randomKeyspace = randomString
    val cluster = Cluster.builder().addContactPoint("localhost").build()
    val session = cluster.connect()
    CassandraHelper.createKeyspace(session, randomKeyspace)
    CassandraHelper.createMetadataTables(session, randomKeyspace)
    session.close()
    cluster.close()
    randomKeyspace
  }

  /**
   * Sorting checker
   */
  def isSorted(list : ListBuffer[UUID]) : Boolean = {
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
   * @param path Zk root to delete
   */
  def removeZkMetadata(path : String) = {
    val zkService = new ZkService("", List(new InetSocketAddress("localhost", 2181)), 7, 7)
    zkService.deleteRecursive(path)
  }

  /**
   * Remove directory recursive
   * @param f Dir to remove
   */
  def remove(f : File) : Unit = {
    if (f.isDirectory) {
      for (c <- f.listFiles())
        remove(c)
    }
    f.delete()
  }
}
