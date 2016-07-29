package com.bwsw.tstreams.common

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import com.bwsw.ResettableCountDownLatch
import com.bwsw.tstreams.common.MandatoryExecutor.MandatoryExecutorException

/**
  * Executor which provides sequence runnable
  * execution but on any failure exception will be thrown
  */
class MandatoryExecutor {
  private val AWAIT_TIMEOUT = 100
  private val awaitSignalVar = new ResettableCountDownLatch(1)
  private val queue = new LinkedBlockingQueue[Runnable]()
  private var executor : Thread = null
  private val isRunning = new AtomicBoolean(true)

  /**
    *
    */
  private def startExecutor() : Unit = {
    //TODO somehow rethrow exception from this thread
    executor = new Thread(new Runnable {
      override def run(): Unit = startInternal()
    })
    executor.start()
  }

  /**
    *
    */
  private def startInternal() : Unit = {
    while (isRunning.get()) {
      val task: Runnable = queue.poll(AWAIT_TIMEOUT, TimeUnit.MILLISECONDS)
      if (task == null) {
        awaitSignalVar.countDown()
        awaitSignalVar.reset()
      }
      else {
        try {
          task.run()
        }
        catch {
          case e: Exception =>
            throw new MandatoryExecutorException(s"Runnable failure with msg { ${e.getMessage} }")
        }
      }
    }
  }

  /**
    *
    * @param task
    */
  def submit(task : Runnable) = {
    queue.add(task)
  }

  /**
    *
    * @return
    */
  def await() = {
    awaitSignalVar.await()
  }
}

/**
  *
  */
object MandatoryExecutor {
  class MandatoryExecutorException(msg : String) extends Exception(msg)
}



