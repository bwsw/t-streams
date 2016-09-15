package transaction_generator

import java.util.UUID

import com.bwsw.tstreams.generator.LocalTimeUUIDGenerator
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}


class LocalTimeCustomUUIDGeneratorTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  "LocalTimeUUIDGenerator.getTimeUUID()" should "return unique UUID" in {
    val gen = new LocalTimeUUIDGenerator
    var uniqueElements = Set[UUID]()
    for (i <- 0 until 100) {
      val prevSize = uniqueElements.size

      val uuid: UUID = gen.getTimeUUID()
      uniqueElements += uuid

      prevSize shouldEqual uniqueElements.size - 1
    }
  }
}
