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

  private val zkService = new ZkService("", List(new InetSocketAddress("localhost",2181)), 7000)
  
  protected val batchSizeTestVal = 5

  def randomString =
    RandomStringCreator.randomAlphaString(10)

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

  def removeZkMetadata(path : String) =
    zkService.deleteRecursive(path)

  def remove(f : File) : Unit = {
    if (f.isDirectory) {
      for (c <- f.listFiles())
        remove(c)
    }
    f.delete()
  }
}
