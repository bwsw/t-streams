package ut.multiNodeServer.ZkTreeListTest

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.metadata.{LedgerMetadata, MetadataRecord, NoRecordReadStatus}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class ZkMultipleTreeListReaderMetadataTest
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll {

  "Metadata record" should "contain timestamp without records" in {
    val metadataRecord = new MetadataRecord(
      Array.empty[LedgerMetadata]
    )

    MetadataRecord.fromByteArray(metadataRecord.toByteArray) shouldBe metadataRecord
  }

  it should "contain timestamp with records" in {
    val recordNumber = 10

    val rand = scala.util.Random
    val records = Array.fill(recordNumber)(
      LedgerMetadata(rand.nextLong(), rand.nextLong(), NoRecordReadStatus)
    )

    val metadataRecord = new MetadataRecord(
      records
    )

    MetadataRecord.fromByteArray(metadataRecord.toByteArray) shouldBe metadataRecord
  }

}
