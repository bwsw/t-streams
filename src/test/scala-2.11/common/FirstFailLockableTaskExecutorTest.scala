package common

import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreams.common.FirstFailLockableTaskExecutor
import com.bwsw.tstreams.common.FirstFailLockableTaskExecutor.FirstFailLockableExecutorException
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}


class FirstFailLockableTaskExecutorTest extends FlatSpec with Matchers with BeforeAndAfterAll{
  val errorMsg = java.util.UUID.randomUUID().toString
  val runnableWithException = new Runnable {
    override def run(): Unit = {
      throw new RuntimeException(errorMsg)
    }
  }

  "Mandatory executor" should "throw exception in case of runnable failure" in {
    val mandatoryExecutor = new FirstFailLockableTaskExecutor

    mandatoryExecutor.submit(runnableWithException)
    mandatoryExecutor.await()
    val msg: Option[Boolean] =
    try {
      mandatoryExecutor.submit(new Runnable {
        override def run(): Unit = ()
      })
      None
    } catch {
      case e : Exception =>
        Option(e.getMessage.contains(errorMsg))
    }
    msg.isDefined shouldBe true
    msg.get shouldBe true
  }

  "Mandatory executor" should "handle all messages and await" in {
    val mandatoryExecutor = new FirstFailLockableTaskExecutor

    var cnt = 0
    val updateRunnable = new Runnable {
      override def run(): Unit = {
        cnt += 1
      }
    }
    0 until 1000 foreach { _ =>
      mandatoryExecutor.submit(updateRunnable)
    }
    mandatoryExecutor.await()
    cnt shouldBe 1000
  }

  "Mandatory executor" should "return instantly after await if there are no runnables to handle" in {
    val mandatoryExecutor = new FirstFailLockableTaskExecutor
    mandatoryExecutor.await()
    //just check that there is no deadlock
    true shouldBe true
  }

  "Mandatory executor" should "handle all messages and await (messages can have long time of execution)" in {
    val mandatoryExecutor = new FirstFailLockableTaskExecutor

    var cnt = 0
    val updateRunnable = new Runnable {
      override def run(): Unit = {
        Thread.sleep(100)
        cnt += 1
      }
    }
    0 until 100 foreach { _ =>
      mandatoryExecutor.submit(updateRunnable)
    }
    mandatoryExecutor.await()
    cnt shouldBe 100
  }

  "Mandatory executor" should "prevent execution of new tasks if one of them failed" in {
    val mandatoryExecutor = new FirstFailLockableTaskExecutor

    var cnt = 0
    val updateRunnable = new Runnable {
      override def run(): Unit = {
        cnt += 1
      }
    }
    0 until 1000 foreach { x =>
      //submit corrupted task
      if (x === 500)
        mandatoryExecutor.submit(runnableWithException)

      try {
        mandatoryExecutor.submit(updateRunnable)
      }
      catch {
        //just ignore
        case e : FirstFailLockableExecutorException =>
      }
    }

    cnt shouldBe 500
  }

  "Mandatory executor" should "release lock of corrupted runnable" in {
    val mandatoryExecutor = new FirstFailLockableTaskExecutor
    val lock = new ReentrantLock(true)
    mandatoryExecutor.submit(runnableWithException, Option(lock))
    mandatoryExecutor.await()
    lock.isLocked shouldBe false
    mandatoryExecutor.isFailed shouldBe true
  }

  "Mandatory executor" should "release await in case of failure" in {
    val mandatoryExecutor = new FirstFailLockableTaskExecutor
    val lock = new ReentrantLock(true)

    var cnt = 0
    val updateRunnable = new Runnable {
      override def run(): Unit = {
        Thread.sleep(1000)
        cnt += 1
      }
    }
    0 until 5 foreach { _ =>
      mandatoryExecutor.submit(updateRunnable)
    }
    mandatoryExecutor.submit(runnableWithException)
    mandatoryExecutor.await()
    cnt shouldBe 5
    mandatoryExecutor.isFailed shouldBe true
    lock.isLocked shouldBe false
  }

  "Mandatory executor" should "await all tasks and throw exception on new submit's in case of shutdown" in {
    val mandatoryExecutor = new FirstFailLockableTaskExecutor
    var cnt = 0
    val updateRunnable = new Runnable {
      override def run(): Unit = {
        Thread.sleep(1000)
        cnt += 1
      }
    }
    0 until 5 foreach { _ =>
      mandatoryExecutor.submit(updateRunnable)
    }
    mandatoryExecutor.shutdownSafe()
    intercept[FirstFailLockableExecutorException] {
      mandatoryExecutor.submit(new Runnable {
        override def run(): Unit = ()
      })
    }
    mandatoryExecutor.isStopped shouldBe true
  }
}