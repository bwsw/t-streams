package common

import java.util.concurrent.CountDownLatch

import com.bwsw.tstreams.common.ResettableCountDownLatch
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 07.08.16.
  */
class ResettableCountDownLatchTests extends FlatSpec with Matchers {
  val resettable = new ResettableCountDownLatch(1)
  var v = 1
  "Countdown/await" should "work properly" in {
    val l = new CountDownLatch(1)
    new Thread(new Runnable {
      override def run(): Unit = {
        resettable.await()
        v *= 2
        l.countDown()
      }
    }).start()
    v += 2
    resettable.countDown
    l.await()
    v shouldBe 6
  }

  "Reinitialization" should "work properly" in {
    resettable.setValue(2)
    val l = new CountDownLatch(1)
    new Thread(new Runnable {
      override def run(): Unit = {
        resettable.await()
        v *= 2
        l.countDown()
      }
    }).start()
    v = 1
    resettable.countDown
    v = 2
    resettable.countDown
    l.await()
    v shouldBe 4
  }
}
