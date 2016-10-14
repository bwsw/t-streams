package common

import java.util.concurrent.atomic.AtomicInteger

import com.bwsw.tstreams.common.{ResourceCountingMap, ResourceSharedBackingSourceMap}
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 14.10.16.
  */
class SharedBackingSourceMapTests extends FlatSpec with Matchers  {
  it should "work properly" in {
    val removeCtr = new AtomicInteger(0)
    val rcm = new ResourceCountingMap[Int, AtomicInteger, Unit]((a: AtomicInteger) => removeCtr.incrementAndGet())
    val sbrm = new ResourceSharedBackingSourceMap[Int, AtomicInteger, Unit](rcm)

    sbrm.put(0, new AtomicInteger(1))

    sbrm.put(0, new AtomicInteger(2))

    val v = sbrm.get(0)

    v.isDefined shouldBe true
    v.get.get shouldBe 2

    v.get.incrementAndGet()

    val bv = rcm.acquire(0)
    bv.get.get shouldBe 3
    rcm.release(0)

    sbrm.remove(0)
    removeCtr.get shouldBe 2

  }
}
