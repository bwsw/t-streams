package com.bwsw.tstreams.common

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 11.06.17.
  */
class RoundRobinPolicyTests extends FlatSpec with Matchers {
  val COUNT = 3
  it should "operate correctly over partitions" in {
    val set = Set(0,1,2)
    val roundRobinPolicy = new RoundRobinPolicy(COUNT, set)
    Seq(0, 1, 2, 0, 1, 2)
      .foreach(partitionExpectation => roundRobinPolicy.getNextPartition() shouldBe partitionExpectation)
  }
}
