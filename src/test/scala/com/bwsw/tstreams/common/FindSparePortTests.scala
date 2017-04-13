package com.bwsw.tstreams.common

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 06.09.16.
  */
class FindSparePortTests extends FlatSpec with Matchers {
  it should "return port is spare for 40000" in {
    SpareServerSocketLookupUtility.findSparePort("0.0.0.0", 40000, 40000).get shouldBe 40000
  }
}
