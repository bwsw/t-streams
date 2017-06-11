package com.bwsw.tstreams.common

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 08.05.17.
  */
class FirstFailLockableTaskExecutorTest extends FlatSpec with Matchers  {
  it should "return valid future and able to get requested object" in {
    val counter = new AtomicInteger(0)
    val executor = new FirstFailLockableTaskExecutor("sample")
    val job = executor.submit("test task", () => counter.incrementAndGet(), None, counter)
    job.get() shouldBe counter

  }
}
