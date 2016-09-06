package common

import com.bwsw.tstreams.common.SpareServerSocketLookupUtility
import org.scalatest.{Matchers, FlatSpec}

/**
  * Created by Ivan Kudryavtsev on 06.09.16.
  */
class FindSparePortTests extends FlatSpec with Matchers  {
  it should "return port is spare for 40000" in {
    SpareServerSocketLookupUtility.findSparePort("0.0.0.0", 40000, 40001).get shouldBe 40000
  }
}
