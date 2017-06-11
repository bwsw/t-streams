package com.bwsw.tstreams.common

import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 07.08.16.
  */
class ResettableCountDownLatchTests extends FlatSpec with Matchers {
  val resettable = new ResettableCountDownLatch(1)
  var expectedValue = 1
  "Countdown/await" should "work properly" in {
    val l = new CountDownLatch(1)
    new Thread(() => {
      resettable.await()
      expectedValue *= 2
      l.countDown()
    }).start()
    expectedValue += 2
    resettable.countDown
    l.await()
    expectedValue shouldBe 6
  }

  "Reinitialization" should "work properly" in {
    resettable.setValue(2)
    val l = new CountDownLatch(1)
    new Thread(() => {
      resettable.await()
      expectedValue *= 2
      l.countDown()
    }).start()
    expectedValue = 1
    resettable.countDown
    expectedValue = 2
    resettable.countDown
    l.await()
    expectedValue shouldBe 4
  }

  "Mass usage" should "work properly" in {
    val THREADS = 200
    val THREAD_COUNT = 100000
    val l = new ResettableCountDownLatch(THREADS * THREAD_COUNT)
    val threads = (0 until THREADS).map(_ => new Thread(() => {
      (0 until THREAD_COUNT).foreach(_ => l.countDown())
    }))
    threads.foreach(t => t.start())

    l.await(10, TimeUnit.SECONDS) shouldBe true

    threads.foreach(t => t.join(1000))

  }
}
