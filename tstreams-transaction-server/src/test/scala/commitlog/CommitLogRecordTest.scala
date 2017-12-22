package commitlog

import com.bwsw.commitlog.CommitLogRecord
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class CommitLogRecordTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  "CommitLogRecord" should "be serialized/deserialized" in {
    val record1 = new CommitLogRecord(0:Byte, "test_data".getBytes())
    CommitLogRecord.fromByteArray(record1.toByteArray).right.get shouldBe record1

    val record2 = new CommitLogRecord(-5:Byte, "".getBytes())
    CommitLogRecord.fromByteArray(record2.toByteArray).right.get shouldBe record2
  }

}
