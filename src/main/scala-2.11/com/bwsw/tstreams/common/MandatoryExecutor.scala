package com.bwsw.tstreams.common

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{CountDownLatch, Executor, LinkedBlockingQueue}

import com.bwsw.ResettableCountDownLatch
import com.bwsw.tstreams.common.MandatoryExecutor.{MandatoryExecutorException, MandatoryExecutorTask}
import org.slf4j.LoggerFactory

/**
  * Executor which provides sequence runnable
  * execution but on any failure exception will be thrown
  */
class MandatoryExecutor extends Executor{
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val awaitSignalVar = new ResettableCountDownLatch(0)
  private val queue = new LinkedBlockingQueue[MandatoryExecutorTask]()
  private val isNotFailed = new AtomicBoolean(true)
  private val isRunning = new AtomicBoolean(true)
  private val isShutdown = new AtomicBoolean(false)
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
        logger.info("[MANDATORY EXECUTOR] starting Mandatory Executor")

        //main task handle cycle
        while (isNotFailed.get() && isRunning.get()) {
          val task: MandatoryExecutorTask = queue.take()
          try {
            task.lock.foreach(x=>x.lock())
            task.runnable.run()
            task.lock.foreach(x=>x.unlock())
          }
          catch {
            case e: Exception =>
              logger.warn("[MANDATORY EXECUTOR] task failure; stop executor")
              task.lock.foreach(x=>x.unlock())
              isNotFailed.set(false)
              failureMessage = e.getMessage
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
    *
    * @param runnable
    */
  def submit(runnable : Runnable, lock : Option[ReentrantLock] = None) = {
    if (isShutdown.get()){
      throw new MandatoryExecutorException("executor is been shutdown")
    }
    if (runnable == null) {
      throw new MandatoryExecutorException("runnable must be not null")
    }
    if (executor != null && !isNotFailed.get()){
      throw new MandatoryExecutorException(failureMessage)
    }
    queue.add(MandatoryExecutorTask(runnable, isIgnorableIfExecutorFailed = true, lock))
  }

  /**
    * Wait all current tasks to be handled
    * Warn! this method is not thread safe
    */
  def await() : Unit = {
    if (isShutdown.get()){
      throw new MandatoryExecutorException("executor is been shutdown")
    }
    if (executor != null && !isNotFailed.get()){
      throw new MandatoryExecutorException(failureMessage)
    }
    this.awaitInternal()
  }

  /**
    * Internal method for [[await]]
    */
  private def awaitInternal() : Unit = {
    awaitSignalVar.setValue(1)
    val runnable = new Runnable {
      override def run(): Unit = {
        awaitSignalVar.countDown()
      }
    }
    queue.add(MandatoryExecutorTask(runnable, isIgnorableIfExecutorFailed = false, lock = None))
    awaitSignalVar.await()
  }

  /**
    * Safe shutdown this executor
    */
  def shutdownSafe() : Unit = {
    logger.info("[MANDATORY EXECUTOR] Start shutdown mandatory executor")
    if (isShutdown.get()){
      throw new MandatoryExecutorException("executor is already been shutdown")
    }
    isShutdown.set(true)
    this.awaitInternal()

    //stop handler thread
    isRunning.set(false)
    //need to skip queue.take() block
    queue.add(MandatoryExecutorTask(
      runnable = new Runnable {
        override def run(): Unit = ()
      },
      isIgnorableIfExecutorFailed = true,
      lock = None))
    executor.join()
    logger.info("[MANDATORY EXECUTOR] Finished shutdown mandatory executor")
  }

  /**
    * Executor state
    * true if one of runnable's threw exception
    * false else
    */
  def isFailed =
    !isNotFailed.get()

  /**
    * True if executor shutdown
    */
  def isStopped =
    isShutdown.get()

  /**
    *
    * @param command
    */
  override def execute(command: Runnable): Unit = {
    this.submit(command)
  }
}

/**
  * Mandatory executor objects
  */
object MandatoryExecutor {
  class MandatoryExecutorException(msg : String) extends Exception(msg)
  sealed case class MandatoryExecutorTask(runnable : Runnable,
                                   isIgnorableIfExecutorFailed : Boolean,
                                   lock : Option[ReentrantLock])
}
