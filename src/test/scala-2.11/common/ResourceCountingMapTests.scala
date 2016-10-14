package common

import java.util.concurrent.atomic.AtomicInteger

import com.bwsw.tstreams.common.ResourceCountingMap
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 14.10.16.
  */
class ResourceCountingMapTests extends FlatSpec with Matchers {

  it should "Do placement and removal if once" in {
    val removeCtr = new AtomicInteger(0)
    val rcm = new ResourceCountingMap[Int, String, Unit]((a: String) => removeCtr.incrementAndGet())
    rcm.place(0, "test")
    val v = rcm.acquire(0)
    v.nonEmpty shouldBe true
    v.get shouldBe "test"
    rcm.release(0)
    removeCtr.get shouldBe 1
  }

  it should "Do placement and proper removal if twice" in {
    val removeCtr = new AtomicInteger(0)
    val rcm = new ResourceCountingMap[Int, String, Unit]((a: String) => removeCtr.incrementAndGet())
    rcm.place(0, "test")
    val v = rcm.acquire(0)
    v.nonEmpty shouldBe true
    v.get shouldBe "test"

    val v1 = rcm.acquire(0)
    v1.nonEmpty shouldBe true
    v1.get shouldBe "test"

    rcm.release(0)
    rcm.release(0)

    removeCtr.get shouldBe 1
  }

  it should "Do placement and proper forced removal if twice" in {
    val removeCtr = new AtomicInteger(0)
    val rcm = new ResourceCountingMap[Int, String, Unit]((a: String) => removeCtr.incrementAndGet())
    rcm.place(0, "test")
    val v = rcm.acquire(0)
    v.nonEmpty shouldBe true
    v.get shouldBe "test"

    val v1 = rcm.acquire(0)
    v1.nonEmpty shouldBe true
    v1.get shouldBe "test"

    rcm.forceRelease(0)
    removeCtr.get shouldBe 1
  }

  it should "Correctly handle double placement (by name)" in {
    val removeCtr = new AtomicInteger(0)
    val addCtr = new AtomicInteger(0)
    val rcm = new ResourceCountingMap[Int, String, Unit]((a: String) => removeCtr.incrementAndGet())
    rcm.place(0, {
      addCtr.addAndGet(1)
      "test"})

    // lazy evaluation must be here... Check it.
    rcm.place(0, {
      addCtr.addAndGet(2)
      "test"})

    addCtr.get shouldBe 1
  }
}
