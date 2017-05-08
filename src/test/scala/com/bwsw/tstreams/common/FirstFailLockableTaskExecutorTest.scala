package com.bwsw.tstreams.common

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.{Matchers, FlatSpec}

/**
  * Created by Ivan Kudryavtsev on 08.05.17.
  */
class FirstFailLockableTaskExecutorTest extends FlatSpec with Matchers  {
  it should "return valid future and able to get requested object" in {
    val cntr = new AtomicInteger(0)
    val ex = new FirstFailLockableTaskExecutor("sample")
    val job = ex.submit("test task", () => cntr.incrementAndGet(), None, cntr)
    job.get() shouldBe cntr

  }
}
