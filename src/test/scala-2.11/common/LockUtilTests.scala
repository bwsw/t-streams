package common

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreams.common.LockUtil
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Created by ivan on 03.08.16.
  */
class LockUtilTests extends FlatSpec with Matchers with BeforeAndAfterAll {
  "After call lockOrDie" should "lock to be received" in {
    val l = new ReentrantLock()
    LockUtil.lockOrDie(l, (100, TimeUnit.MILLISECONDS))
    l.getHoldCount shouldBe 1
    l.unlock()
  }

  "After withLockOrDieDo" should "lock to be received and released" in {
    val l = new ReentrantLock()
    LockUtil.withLockOrDieDo[Unit](l, (100, TimeUnit.MILLISECONDS), None, () => {
      l.getHoldCount shouldBe 1
    })
    l.getHoldCount shouldBe 0
  }


  "After withLockOrDieDo" should "lock to be received and released even with exception" in {
    val l = new ReentrantLock()
    try {
      LockUtil.withLockOrDieDo[Unit](l, (100, TimeUnit.MILLISECONDS), None, () => {
        l.getHoldCount shouldBe 1
        throw new Exception("expected")
      })
    } catch {
      case e: Exception =>
        e.getMessage shouldBe "expected"
        l.getHoldCount shouldBe 0
    }
  }

}
