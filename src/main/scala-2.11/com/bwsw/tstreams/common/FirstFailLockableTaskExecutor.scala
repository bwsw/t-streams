package com.bwsw.tstreams.common

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{TimeUnit, CountDownLatch, Executor, LinkedBlockingQueue}

import com.bwsw.ResettableCountDownLatch
import com.bwsw.tstreams.common.FirstFailLockableTaskExecutor.{FirstFailLockableExecutorException, FirstFailLockableExecutorTask}
import org.slf4j.LoggerFactory

/**
  * Executor which provides sequential runnable
  * execution but stops after the first exception
  */

class FirstFailLockableTaskExecutor extends Executor {

  val isRunning = new AtomicBoolean(true)

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val awaitSignalVar = new ResettableCountDownLatch(0)
  private val queue = new LinkedBlockingQueue[FirstFailLockableExecutorTask]()
  private val isNotFailed = new AtomicBoolean(true)
  private val isShutdown = new AtomicBoolean(false)
  private var executor : Thread = null
  private var failureExc : Exception = null
  private val lock: ReentrantLock = new ReentrantLock()
  startExecutor()

  /**
    * task handler
    */
  private def startExecutor() : Unit = {
    val latch = new CountDownLatch(1)
    executor = new Thread(new Runnable {
      override def run(): Unit = {
        latch.countDown()
        logger.info("[FIRSTFAILLOCKABLE EXECUTOR] starting")

        //main task handle cycle
        while (isNotFailed.get() && isRunning.get()) {
          val task: FirstFailLockableExecutorTask = queue.take()
          try {
            task.lock.foreach(x=>x.lock())
            task.runnable.run()
            task.lock.foreach(x=>x.unlock())
          }
          catch {
            case e: Exception =>
              logger.warn("[FIRSTFAILLOCKABLE EXECUTOR] task failure; stop executor")
              task.lock.foreach(x=>x.unlock())
              isNotFailed.set(false)
              failureExc = e
              logger.error(failureExc.getMessage)
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
      throw new FirstFailLockableExecutorException("executor is been shutdown")
    }
    if (runnable == null) {
      throw new FirstFailLockableExecutorException("runnable must be not null")
    }
    if (executor != null && !isNotFailed.get()){
      throw new FirstFailLockableExecutorException(failureExc.getMessage)
    }
    queue.add(FirstFailLockableExecutorTask(runnable, isIgnorableIfExecutorFailed = true, lock))
  }

  /**
    * Wait all current tasks to be handled
    * Warn! this method is not thread safe
    */
  def await() : Unit = {
    if (isShutdown.get()){
      throw new FirstFailLockableExecutorException("executor is been shutdown")
    }
    if (executor != null && !isNotFailed.get()){
      throw new FirstFailLockableExecutorException(failureExc.getMessage)
    }
    this.awaitInternal()
  }

  private def getRunnable: Runnable =
    new Runnable {
      override def run(): Unit = {
        awaitSignalVar.countDown()
      }
    }


  /**
    * Internal method for [[await]]
    */
  private def awaitInternal() : Unit = {
    awaitSignalVar.setValue(1)
    queue.add(FirstFailLockableExecutorTask(getRunnable, isIgnorableIfExecutorFailed = false, lock = None))
    logger.info("before await")
    awaitSignalVar.await()
  }

  /**
    * Safe shutdown this executor
    */
  def shutdownSafe() : Unit = {
    logger.info("[FIRSTFAILLOCKABLE EXECUTOR] Started shutting down the executor")
    if (isShutdown.get()){
      throw new FirstFailLockableExecutorException("executor is already been shutdown")
    }
    isShutdown.set(true)
    this.awaitInternal()

    //stop handler thread
    isRunning.set(false)
    //need to skip queue.take() block
    queue.add(FirstFailLockableExecutorTask(
      runnable = new Runnable {
        override def run(): Unit = ()
      },
      isIgnorableIfExecutorFailed = true,
      lock = None))
    executor.join()
    logger.info("[FIRSTFAILLOCKABLE EXECUTOR] Finished shutting down the executor")
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
  * Implements pool of executors
  */
class FirstFailLockableTaskExecutorPool(val size: Int = 4) extends Executor {

  private val pool = new Array[FirstFailLockableTaskExecutor](size)
  private var next: Int = 0
  private val lock = new ReentrantLock()
  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * send runnable for queueud execution
    * @param command
    */
  override def execute(command: Runnable): Unit = {
    LockUtil.lockOrDie(lock, (100, TimeUnit.SECONDS), Some(logger))
    pool(next).submit(command)
    next += 1
    next = next % size
    lock.unlock()
  }

  /**
    * await for all tasks on all executors will be completed
    */
  def await(): Unit = {
    LockUtil.lockOrDie(lock, (100, TimeUnit.SECONDS), Some(logger))
    (0 until size).foreach(x => pool(x).await)
    lock.unlock()
  }

  /**
    * shutdown pool correctly
    */
  def shutdownSafe(): Unit = {
    LockUtil.lockOrDie(lock, (100, TimeUnit.SECONDS), Some(logger))
    (0 until size).foreach(x => pool(x).isRunning.set(false))
    (0 until size).foreach(x => pool(x).shutdownSafe)
    lock.unlock()
  }

}

/**
  * FirstFailLockable executor objects
  */
object FirstFailLockableTaskExecutor {
  class FirstFailLockableExecutorException(msg : String) extends Exception(msg)
  sealed case class FirstFailLockableExecutorTask(runnable : Runnable,
                                                  isIgnorableIfExecutorFailed : Boolean,
                                                  lock : Option[ReentrantLock])
}
