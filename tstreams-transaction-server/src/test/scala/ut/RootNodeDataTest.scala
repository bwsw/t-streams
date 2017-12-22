package ut

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy.RootNodeData
import org.scalatest.{FlatSpec, Matchers}

class RootNodeDataTest
  extends FlatSpec
    with Matchers {

  it should "be serialized/deserialized correctly" in {
    val first = "Tests"
    val second = "Good for you"

    val rootNodeData =
      RootNodeData(first.getBytes(), first.getBytes())

    RootNodeData.fromByteArray(rootNodeData.toByteArray) shouldBe rootNodeData
  }
}
