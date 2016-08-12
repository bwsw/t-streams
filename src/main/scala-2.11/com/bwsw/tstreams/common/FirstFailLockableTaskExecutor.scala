package com.bwsw.tstreams.common

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{TimeUnit, CountDownLatch, Executor, LinkedBlockingQueue}

import com.bwsw.tstreams.common.FirstFailLockableTaskExecutor.{FirstFailLockableExecutorException, FirstFailLockableExecutorTask}
import org.slf4j.LoggerFactory

/**
  * Executor which provides sequential runnable
  * execution but stops after the first exception
  */

class FirstFailLockableTaskExecutor(name: String) extends Executor {

  private val isRunning = new AtomicBoolean(true)
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
        Thread.currentThread().setName(name)
        latch.countDown()
        logger.debug("[FIRSTFAILLOCKABLE EXECUTOR] starting")

        //main task handle cycle
        while (isNotFailed.get() && isRunning.get()) {
          val task: FirstFailLockableExecutorTask = queue.take()
          LockUtil.withLockOrDieDo[Unit](lock, (100, TimeUnit.SECONDS), Some(logger), () => {
            try {
              task.lock.foreach(x => x.lock())
              task.runnable.run()
              task.lock.foreach(x => x.unlock())
            }
            catch {
              case e: Exception =>
                logger.warn("[FIRSTFAILLOCKABLE EXECUTOR] task failure; stop executor")
                task.lock.foreach(x => x.unlock())
                isNotFailed.set(false)
                failureExc = e
                if(System.getProperty("DEBUG","false") == "true")
                  throw e
            }
          })
        }

        //release await in case of executor failure
        LockUtil.withLockOrDieDo[Unit](lock, (100, TimeUnit.SECONDS), Some(logger), () => {
          while (queue.size() > 0) {
            val task = queue.take()
            if (!task.isIgnorableIfExecutorFailed) {
              task.runnable.run()
            }
          }
        })
        logger.debug("[FIRSTFAILLOCKABLE EXECUTOR] thread end")
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
  def submit(runnable : Runnable, l : Option[ReentrantLock] = None) = {
    LockUtil.withLockOrDieDo[Unit](lock, (100, TimeUnit.SECONDS), Some(logger), () => {
      if (isShutdown.get()){
        throw new FirstFailLockableExecutorException(s"Executor ${name} is shut down.")
      }
      if (runnable == null) {
        throw new FirstFailLockableExecutorException("Executor ${name} - runnable must be not null")
      }
      if (executor != null && !isNotFailed.get()){
        throw new FirstFailLockableExecutorException(failureExc.getMessage)
      }
      queue.add(FirstFailLockableExecutorTask(runnable, isIgnorableIfExecutorFailed = true, l))
    })
  }

  /**
    * Wait all current tasks to be handled
    * Warn! this method is not thread safe
    */
  def awaitCurrentTasksWillComplete() : Unit = {
    LockUtil.withLockOrDieDo[Unit](lock, (100, TimeUnit.SECONDS), Some(logger), () => {
      if (isShutdown.get()) {
        throw new FirstFailLockableExecutorException("executor is been shutdown")
      }
      if (executor != null && !isNotFailed.get()) {
        throw new FirstFailLockableExecutorException(failureExc.getMessage)
      }
    })
    this.awaitInternal()
  }

  private def getRunnable: Runnable = new Runnable {
    override def run(): Unit = awaitSignalVar.countDown
  }

  /**
    * Internal method for [[awaitCurrentTasksWillComplete]]
    */
  private def awaitInternal() : Unit = {
    LockUtil.withLockOrDieDo[Unit](lock, (100, TimeUnit.SECONDS), Some(logger), () => {
      if (isFailed) {
        return
      }
      awaitSignalVar.setValue(1)
      queue.add(FirstFailLockableExecutorTask(getRunnable, isIgnorableIfExecutorFailed = false, lock = None))
      logger.debug("[FIRSTFAILLOCKABLE EXECUTOR] Before await signal var to be triggered.")
    })
    awaitSignalVar.await()
  }

  /**
    * Safe shutdown this executor
    */
  def shutdownSafe() : Unit = {
    logger.debug("[FIRSTFAILLOCKABLE EXECUTOR] Started shutting down the executor")
    if (isShutdown.get()){
      throw new FirstFailLockableExecutorException("executor is already been shutdown")
    }
    logger.debug("[FIRSTFAILLOCKABLE EXECUTOR] Before awaiting current tasks will be terminated")
    this.awaitInternal()
    logger.debug("[FIRSTFAILLOCKABLE EXECUTOR] After awaiting current tasks will be terminated")

    LockUtil.withLockOrDieDo[Unit](lock, (100, TimeUnit.SECONDS), Some(logger), () => {
      isShutdown.set(true)
      //stop handler thread
      isRunning.set(false)

      //need to skip queue.take() block
      queue.add(FirstFailLockableExecutorTask(
        runnable = new Runnable {
          override def run(): Unit = ()
        },
        isIgnorableIfExecutorFailed = true,
        lock = None))
    })
    executor.join()
    logger.debug("[FIRSTFAILLOCKABLE EXECUTOR] Finished shutting down the executor")
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

  def getException: Option[Exception] = Some(failureExc)
}

/**
  * Implements pool of executors
  */
class FirstFailLockableTaskExecutorPool(val name: String, val size: Int = 4) extends Executor {

  private val pool = new Array[FirstFailLockableTaskExecutor](size)
  (0 until size).foreach(i => pool(i) = new FirstFailLockableTaskExecutor(s"${name}-pool-${i}"))
  private var next: Int = 0
  private val lock = new ReentrantLock()
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val isShutdown = new AtomicBoolean(false)


  /**
    * send runnable for queueud execution
    *
    * @param command to run at executor pool
    */
  override def execute(command: Runnable): Unit = {
    if(isShutdown.get())
      throw new FirstFailLockableExecutorException("ExecutorPool is already been shutdown")
    LockUtil.withLockOrDieDo[Unit](lock, (100, TimeUnit.SECONDS), Some(logger), () => {
      logger.debug("Try to add task for execution")
      var isFailed = false

      (0 until size).foreach(x => {
        if (pool(x).isFailed) {
          logger.debug("Task execution is denied because previous execution failed")
          if (pool(x).getException.isDefined)
            throw pool(x).getException.get
          else
            throw new IllegalStateException(s"Thread-${x} in the pool meet failed task. The pool state is failed, but task exception is unknown.")
        }
      })
      pool(next).submit(command)
      next += 1
      next = next % size
      logger.debug("Task is scheduled for execution")
    })
  }

  /**
    * await for all tasks on all executors will be completed
    */
  def awaitCurrentTasksWillComplete(): Unit = {
    if(isShutdown.get())
      throw new FirstFailLockableExecutorException("ExecutorPool is already been shutdown")
    LockUtil.withLockOrDieDo[Unit](lock, (100, TimeUnit.SECONDS), Some(logger), () => {
      (0 until size).foreach(x => pool(x).awaitCurrentTasksWillComplete)
    })
  }

  /**
    * shutdown pool correctly
    */
  def shutdownSafe(): Unit = {
    if(isShutdown.get())
      throw new FirstFailLockableExecutorException("ExecutorPool is already been shutdown")
    LockUtil.withLockOrDieDo[Unit](lock, (100, TimeUnit.SECONDS), Some(logger), () => {
      logger.debug("Pool started to shut down")
      isShutdown.set(true)
      (0 until size).foreach(x => pool(x).shutdownSafe)
      logger.debug("Pool is shut down")
    })
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
