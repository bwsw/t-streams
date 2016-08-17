package common

import com.bwsw.tstreams.common.CassandraHelper
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import testutils.TestUtils

/**
  * Created by Ivan A. Kudryavtsev according to Issue TS-213 on 17.08.16.
  */
class CassandraHelperTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils  {
  val ks = "ks_920b56599f40"

  "A keyspace" should "be created" in {
    CassandraHelper.createKeyspace(session, ks)
  }

  "A keyspace" should "be destroyed" in {
    CassandraHelper.dropKeyspace(session, ks)
  }

  override def afterAll(): Unit = {
    onAfterAll()
  }
}
