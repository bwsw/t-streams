package com.bwsw.tstreams.common

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Ivan A. Kudryavtsev on 11.06.17.
  */
class AbstractPolicyTests extends FlatSpec with Matchers {
  val COUNT = 3

  class TestAbstractPolicy(count: Int, set: Set[Int]) extends AbstractPolicy(count, set) {
    override def getNextPartition: Int = 0
  }

  it should "handle proper partition set correctly" in {
    val set = Set(0,1,2)
    new TestAbstractPolicy(COUNT, set)
  }

  it should "handle improper partition set correctly" in {
    val set = Set(0,1,4)
    intercept[IllegalArgumentException] {
      new TestAbstractPolicy(COUNT, set)
    }
  }

  it should "handle correctly empty set" in {
    val set = Set.empty[Int]
    intercept[IllegalArgumentException] {
      new TestAbstractPolicy(COUNT, set)
    }
  }

  it should "handle getCurrentPartition correctly" in {
    val set = Set(0,1,2)
    val p = new TestAbstractPolicy(COUNT, set)
    p.getCurrentPartition shouldBe 0
  }

  it should "handle startNewRound correctly" in {
    val set = Set(0,1,2)
    val p = new TestAbstractPolicy(COUNT, set)
    p.startNewRound()
    p.getCurrentPartition shouldBe 0
  }
}
