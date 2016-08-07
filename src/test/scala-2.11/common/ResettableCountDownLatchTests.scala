package common

import com.bwsw.tstreams.common.ResettableCountDownLatch
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by ivan on 07.08.16.
  */
class ResettableCountDownLatchTests extends FlatSpec with Matchers {
  val l = new ResettableCountDownLatch(1)
  var v = 1
  "Countdown/await" should "work properly" in {
    val t = new Thread(new Runnable {
      override def run(): Unit = l.await()
      v *= 2
    })
    v = 0
    l.countDown
    v shouldBe 0
  }

  "Reinitialization" should "work properly" in {
    l.setValue(2)
    val t = new Thread(new Runnable {
      override def run(): Unit = l.await()
      v *= 2
    })
    v = 1
    l.countDown
    v = 0
    l.countDown
    v shouldBe 0
  }
}
