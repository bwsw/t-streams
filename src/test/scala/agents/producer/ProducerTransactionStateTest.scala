package agents.producer

import java.util.concurrent.CountDownLatch

import com.bwsw.tstreams.agents.producer.ProducerTransactionState
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 06.08.16.
  */
class ProducerTransactionStateTest extends FlatSpec with Matchers {
  "Proper state update process" should "return ordered results" in {
    val s = new ProducerTransactionState
    val v = new Array[Int](3)
    v.foreach(i => v(i) = -1)
    var idx = 0
    val l = new CountDownLatch(1)
    val t = new Thread(() => {
      s.setUpdateInProgress
      v(idx) = 1
      idx += 1
      l.countDown()
      v(idx) = 2
      idx += 1
      s.setUpdateFinished
    })
    t.run()
    l.await()
    s.awaitUpdateComplete
    v(idx) = 3

    v(0) shouldBe 1
    v(1) shouldBe 2
    v(2) shouldBe 3
  }

  "Call closeOrDie twice" should "return ok first time, exception second time" in {
    val s = new ProducerTransactionState
    s.closeOrDie
    var f = false
    try {
      s.closeOrDie
    } catch {
      case e: IllegalStateException =>
        f = true
    }
    f shouldBe true
  }

  "Call isClosed on fresh object" should "return false" in {
    val s = new ProducerTransactionState
    s.isClosed shouldBe false
  }

  "Call isOpenedOrDie" should "return ok before close, exception second time" in {
    val s = new ProducerTransactionState
    s.isOpenedOrDie
    s.closeOrDie
    var f = false
    try {
      s.isOpenedOrDie
    } catch {
      case e: IllegalStateException =>
        f = true
    }
    f shouldBe true
  }

  "Transaction state materialization" should "work in ordered way" in {
    val s = new ProducerTransactionState
    val v = new Array[Int](2)
    v.foreach(i => v(i) = -1)
    var idx = 0
    val l = new CountDownLatch(1)

    val t = new Thread(() => {
      l.countDown()
      v(idx) = 1
      idx += 1
      s.makeMaterialized
    })
    t.run()
    l.await()
    s.awaitMaterialization(5)
    v(idx) = 2

    v(0) shouldBe 1
    v(1) shouldBe 2
  }

}
