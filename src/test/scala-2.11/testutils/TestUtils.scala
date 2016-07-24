package testutils

import java.io.File
import java.net.InetSocketAddress
import java.util.UUID
import com.bwsw.tstreams.common.zkservice.ZkService
import scala.collection.mutable.ListBuffer

/**
 * Test help utils
 */
trait TestUtils {
  // TODO: remove that trash!
  private val zkService = new ZkService("", List(new InetSocketAddress("localhost",2181)), 7, 7)
  protected val batchSizeTestVal = 5

  /**
   * Random alpha string generator
   * @return Alpha string
   */
  def randomString =
    RandomStringCreator.randomAlphaString(10)

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
  def removeZkMetadata(path : String) =
    zkService.deleteRecursive(path)

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
