package common

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreams.common.FirstFailLockableTaskExecutor.FirstFailLockableExecutorException
import com.bwsw.tstreams.common.FirstFailLockableTaskExecutorPool
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}

/**
  * Created by ivan on 02.08.16.
  */
class FirstFailLockableTaskExecutorPoolTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  System.setProperty("DEBUG", "false")
  //System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "DEBUG");

  "Constructor" should "create pool" in {
    val p = new FirstFailLockableTaskExecutorPool("test-group")
    p != null shouldBe true
    p.shutdownSafe()
  }

  "awaitCurrentTasksWillComplete" should "wait for task to complete" in {
    val p = new FirstFailLockableTaskExecutorPool("test-group")
    val v = new AtomicBoolean(false)
    p.execute(new Runnable {
      override def run(): Unit = v.set(true)
    })
    p.awaitCurrentTasksWillComplete()
    v.get shouldBe true
    p.shutdownSafe()
  }

  "If send more than 4 tasks" should "result to required number of executors (4)" in {
    val s = collection.mutable.Set[Thread]()
    val p = new FirstFailLockableTaskExecutorPool("test-group",4)
    (0 until 8).foreach(_ => {
      p.execute(new Runnable {
        override def run(): Unit = s += Thread.currentThread()
      })
      p.awaitCurrentTasksWillComplete()})

    var c = 0
    for (elem <- s) { c+= 1}
    c shouldBe 4
    p.shutdownSafe()
  }

  "If send task which raises exception" should "result to exception when awaitCurrentTasksWillComplete" in {
    val p = new FirstFailLockableTaskExecutorPool("test-group",4)
    val l = new CountDownLatch(1)

    try {
      // this must pass ok
      p.execute(new Runnable {
        override def run(): Unit = {
          l.countDown()
          throw new IllegalArgumentException("expected")
        }
      })
      l.await()
      p.awaitCurrentTasksWillComplete()
    } catch {
      case e: FirstFailLockableExecutorException =>
        e.getMessage shouldBe "expected"
      case e: Exception =>
        throw e
    }
    p.shutdownSafe()
  }

  "If send task which raises exception" should "result to exception when next task is sent" in {

    val p = new FirstFailLockableTaskExecutorPool("test-group",4)
    try {
      // this must pass ok
      p.execute(new Runnable {
        override def run(): Unit = throw new IllegalArgumentException("expected")
      })

      p.execute(new Runnable {
        override def run(): Unit = {}
      })
    } catch {
      case e: IllegalArgumentException =>
        e.getMessage shouldBe "expected"
      case e: Exception =>
        throw e
    }
    p.shutdownSafe()
  }


}
