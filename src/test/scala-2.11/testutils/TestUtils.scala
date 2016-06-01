package testutils

import java.net.InetSocketAddress
import java.util.UUID
import java.util.logging.LogManager

import com.bwsw.tstreams.common.zkservice.ZkService

import scala.collection.mutable.ListBuffer


/**
 * Trait for defining batch size for testing purposes
 */
trait TestUtils {

  private val zkService = new ZkService("", List(new InetSocketAddress("localhost",2181)), 7000)

  /**
   * Current testing batch value
   */
  protected val batchSizeVal = 5

  def randomString = RandomStringCreator.randomAlphaString(10)

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

  def removeZkMetadata() = zkService.deleteRecursive("/unit")
}
