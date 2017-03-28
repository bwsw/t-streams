package agents.producer

import java.util.concurrent.CountDownLatch

import com.bwsw.tstreams.agents.producer.MaterializationGovernor
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 28.08.16.
  */
class MaterializationGovernorTests extends FlatSpec with Matchers {
  it should "init, protect, unprotect, await unprotection in single thread" in {
    val mg = new MaterializationGovernor(Set(0))
    mg.protect(0)
    mg.unprotect(0)
    mg.awaitUnprotected(0)
  }

  it should "init, protect, unprotect, await unprotection in two threads" in {
    val mg = new MaterializationGovernor(Set(0))
    mg.protect(0)
    var end: Long = 0
    val l = new CountDownLatch(1)
    val t = new Thread(() => {
      mg.awaitUnprotected(0)
      end = System.currentTimeMillis()
      l.countDown()
    })
    t.start()
    val start = System.currentTimeMillis()
    Thread.sleep(10)
    mg.unprotect(0)
    l.await()
    end - start >= 10 shouldBe true
  }

  it should "complete await unprotection immediately if no protection set" in {
    val mg = new MaterializationGovernor(Set(0))
    val start = System.currentTimeMillis()
    mg.awaitUnprotected(0)
    val end = System.currentTimeMillis()
    end - start < 5 shouldBe true
  }

}
