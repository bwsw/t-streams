package agents.integration

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.TestUtils

/**
  * Created by mendelbaum_ma on 02.09.16.
  */
class IntersectingTransactionsTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  it should "be ok" in {
    true shouldBe true
  }

  override def afterAll(): Unit = {
    onAfterAll()
  }
}