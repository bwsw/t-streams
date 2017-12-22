package ut

import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.{FileKey, FileValue}
import org.scalatest.{FlatSpec, Matchers}

class CommitLogRocksTest
  extends FlatSpec
    with Matchers {

  "File ID" should "be serialized/deserialized" in {
    val key1 = FileKey(5L)
    FileKey.fromByteArray(key1.toByteArray) shouldBe key1

    val key2 = FileKey(Long.MinValue)
    FileKey.fromByteArray(key2.toByteArray) shouldBe key2

    val key3 = FileKey(Long.MaxValue)
    FileKey.fromByteArray(key3.toByteArray) shouldBe key3
  }

  "File Value" should "be serialized/deserialized" in {
    val fileValue1 = FileValue(new String("test_data_to_check").getBytes, None)
    val fileValue1FromByteArray = FileValue.fromByteArray(fileValue1.toByteArray)
    fileValue1 shouldBe fileValue1FromByteArray

    val fileValue2 = FileValue(new String("test_dat_to_check").getBytes, Some(Array.fill(32)(1:Byte)))
    val fileValue2FromByteArray = FileValue.fromByteArray(fileValue2.toByteArray)
    fileValue2 shouldBe fileValue2FromByteArray

    val fileValue3 = FileValue(Array(0:Byte), None)
    val fileValue3FromByteArray = FileValue.fromByteArray(fileValue3.toByteArray)
    fileValue3 shouldBe fileValue3FromByteArray
  }

}
