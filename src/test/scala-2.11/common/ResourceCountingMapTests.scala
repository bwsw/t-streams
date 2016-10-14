package common

import java.util.concurrent.atomic.AtomicInteger

import com.bwsw.tstreams.common.ResourceCountingMap
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 14.10.16.
  */
class ResourceCountingMapTests extends FlatSpec with Matchers {

  it should "Do placement" in {
    val removeCtr = new AtomicInteger(0)
    val rcm = new ResourceCountingMap[Int, String, Unit]((a: String) => removeCtr.incrementAndGet())
    rcm.place(0, "test")
    val v = rcm.acquire(0)
    v.nonEmpty shouldBe true
    v.get shouldBe "test"
    rcm.release(0)
    removeCtr.get shouldBe 1
  }
}
