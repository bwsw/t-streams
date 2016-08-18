package com.bwsw.tstreams.common

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent._

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.slf4j.LoggerFactory

/**
  * Implements the executor which analyzes task exception, unlocks if necessary and
  * stops further execution immediately if task is failed.
  * @param name executor thread name
  */
class FirstFailLockableTaskExecutor(name: String, cnt: Int = 1)
  extends ThreadPoolExecutor(cnt, cnt, 0, TimeUnit.MILLISECONDS,
    new LinkedBlockingQueue[Runnable],
    new ThreadFactoryBuilder().setNameFormat(s"${name}-%d").build()) {

  import FirstFailLockableTaskExecutor._

  val isFailed = new AtomicBoolean(false)
  private var failureExc: Throwable = null

  /**
    * Allows to determine that task was executed with errors and prevents further task to be executed
    * @param runnable
    * @param throwable
    */
  override def afterExecute(runnable: Runnable, throwable: Throwable): Unit =  {
    super.afterExecute(runnable, throwable)
    if(throwable != null) {
      this.shutdownNow()
      failureExc = throwable
      isFailed.set(true)
      throwable.getStackTrace.foreach(ste => logger.error(ste.toString))
    }
  }

  /**
    * submit task for execution with or without lock
    * @param runnable
    * @param l
    */
  def submit(runnable : Runnable, l : Option[ReentrantLock] = None) = {
    if(isShutdown)
      throw new FirstFailLockableTaskExecutorException(s"Executor ${name} is no longer online. Unable to execute.")
    l.fold(super.execute(runnable)) { lock =>
      super.execute(RunnableWithLock(runnable, lock))
    }
  }

  /**
    *
    * @param runnable
    * @return
    */
  override def submit(runnable: Runnable): Future[_] = {
    if(isShutdown)
      throw new FirstFailLockableTaskExecutorException(s"Executor ${name} is no longer online. Unable to execute.")
    super.submit(runnable)
  }

  /**
    *
    * @param runnable
    */
  override def execute(runnable: Runnable) = {
    if(isShutdown)
      throw new FirstFailLockableTaskExecutorException(s"Executor ${name} is no longer online. Unable to execute.")
    super.execute(runnable)
  }

  /**
    * safely shutdown or die
    */
  def shutdownOrDie(amount: Long, tu: TimeUnit) = {
    this.shutdown()
    if(!this.awaitTermination(amount, tu))
      throw new FirstFailLockableTaskExecutorException(s"Executor service with ${name} was unable to shut down in ${amount} ${tu}")
  }
}

object FirstFailLockableTaskExecutor {
  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * if lock is provided this class wraps runnable with lock
    * @param r Runnable to run wrapped
    * @param lock Lock
    */
  case class RunnableWithLock(r: Runnable, lock: ReentrantLock) extends Runnable {
    override def run(): Unit = LockUtil.withLockOrDieDo[Unit](lock, (100, TimeUnit.SECONDS), Some(logger), () => r.run())
  }

  /**
    *
    * @param msg
    */
  class FirstFailLockableTaskExecutorException(msg : String) extends Exception(msg)
}
