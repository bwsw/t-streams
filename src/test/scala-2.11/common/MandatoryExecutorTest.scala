package common

import com.bwsw.tstreams.common.MandatoryExecutor
import com.bwsw.tstreams.common.MandatoryExecutor.MandatoryExecutorException
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}


class MandatoryExecutorTest extends FlatSpec with Matchers with BeforeAndAfterAll{
  val errorMsg = java.util.UUID.randomUUID().toString
  val runnableWithException = new Runnable {
    override def run(): Unit = {
      throw new RuntimeException(errorMsg)
    }
  }

  "Mandatory executor" should "throw exception in case of runnable failure" in {
    val mandatoryExecutor = new MandatoryExecutor

    mandatoryExecutor.submit(runnableWithException)
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
    val mandatoryExecutor = new MandatoryExecutor

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
    val mandatoryExecutor = new MandatoryExecutor
    mandatoryExecutor.await()
    //just check that there is no deadlock
    true shouldBe true
  }

  "Mandatory executor" should "handle all messages and await (messages can have long time of execution)" in {
    val mandatoryExecutor = new MandatoryExecutor

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
    val mandatoryExecutor = new MandatoryExecutor

    var cnt = 0
    val updateRunnable = new Runnable {
      override def run(): Unit = {
        cnt += 1
      }
    }
    0 until 1000 foreach { x =>
      //submit correpted task
      if (x === 500)
        mandatoryExecutor.submit(runnableWithException)

      try {
        mandatoryExecutor.submit(updateRunnable)
      }
      catch {
        //just ignore
        case e : MandatoryExecutorException =>
      }
    }

    cnt shouldBe 500
  }
}
