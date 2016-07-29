package com.bwsw.tstreams.common

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, TimeUnit}

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
  private val isRunning = new AtomicBoolean(true)
  private var executor : Thread = null
  private var message : String = null
  startExecutor()

  /**
    *
    */
  private def startExecutor() : Unit = {
    val latch = new CountDownLatch(1)
    executor = new Thread(new Runnable {
      override def run(): Unit = {
        latch.countDown()
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
                isRunning.set(false)
                message = e.getMessage
            }
          }
        }
      }
    })
    executor.start()
    latch.await()
  }

  /**
    *
    * @param task
    */
  def submit(task : Runnable) = {
    if (executor != null && !executor.isAlive){
      throw new MandatoryExecutorException(message)
    }
    queue.add(task)
  }

  /**
    *
    * @return
    */
  def await() = {
    if (executor != null && !executor.isAlive){
      throw new MandatoryExecutorException(message)
    }
    awaitSignalVar.await()
  }
}

/**
  *
  */
object MandatoryExecutor {
  class MandatoryExecutorException(msg : String) extends Exception(msg)
}



