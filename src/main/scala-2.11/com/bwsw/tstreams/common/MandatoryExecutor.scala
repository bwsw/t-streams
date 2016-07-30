package com.bwsw.tstreams.common

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue}

import com.bwsw.ResettableCountDownLatch
import com.bwsw.tstreams.common.MandatoryExecutor.{MandatoryExecutorException, MandatoryExecutorTask}

/**
  * Executor which provides sequence runnable
  * execution but on any failure exception will be thrown
  */
class MandatoryExecutor {
  private val awaitSignalVar = new ResettableCountDownLatch(0)
  private val queue = new LinkedBlockingQueue[MandatoryExecutorTask]()
  private val isRunning = new AtomicBoolean(true)
  private var executor : Thread = null
  private var failureMessage : String = null
  startExecutor()

  /**
    * Mandatory task handler
    */
  private def startExecutor() : Unit = {
    val latch = new CountDownLatch(1)
    executor = new Thread(new Runnable {
      override def run(): Unit = {
        latch.countDown()

        //main task handle cycle
        while (isRunning.get()) {
          val task: MandatoryExecutorTask = queue.take()
          if (task == null) {
            awaitSignalVar.countDown()
            awaitSignalVar.reset()
          }
          else {
            try {
              task.runnable.run()
            }
            catch {
              case e: Exception =>
                isRunning.set(false)
                failureMessage = e.getMessage
            }
          }
        }

        //release await in case of executor failure
        while (queue.size() > 0){
          val task = queue.take()
          if (!task.isIgnorableIfExecutorFailed){
            task.runnable.run()
          }
        }
      }
    })
    executor.start()
    latch.await()
  }

  /**
    * Submit new task to execute
    * @param runnable
    */
  def submit(runnable : Runnable) = {
    if (runnable == null) {
      throw new MandatoryExecutorException("runnable must be not null")
    }
    if (executor != null && !executor.isAlive){
      throw new MandatoryExecutorException(failureMessage)
    }
    queue.add(MandatoryExecutorTask(runnable, isIgnorableIfExecutorFailed = true))
  }

  /**
    * Wait all current tasks to be handled
    * Warn! this method is not thread safe
    */
  def await() : Unit = {
    if (executor != null && !executor.isAlive){
      throw new MandatoryExecutorException(failureMessage)
    }
    awaitSignalVar.setValue(1)
    queue.add(MandatoryExecutorTask(new Runnable {
      override def run(): Unit = {
        awaitSignalVar.countDown()
      }
    }, isIgnorableIfExecutorFailed = false))
    awaitSignalVar.await()
  }
}

/**
  * Mandatory executor objects
  */
object MandatoryExecutor {
  class MandatoryExecutorException(msg : String) extends Exception(msg)
  case class MandatoryExecutorTask(runnable : Runnable, isIgnorableIfExecutorFailed : Boolean)
}





