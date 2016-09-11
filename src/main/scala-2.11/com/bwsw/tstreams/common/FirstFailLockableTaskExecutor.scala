package com.bwsw.tstreams.common

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.slf4j.LoggerFactory

/**
  * Implements the executor which analyzes task exception, unlocks if necessary and
  * stops further execution immediately if task is failed.
  * @param name
  */
class FirstFailLockableTaskExecutor(name: String, cnt: Int = 1)
  extends ThreadPoolExecutor(cnt, cnt, 0, TimeUnit.MILLISECONDS,
    new LinkedBlockingQueue[Runnable],
    new ThreadFactoryBuilder().setNameFormat(s"${name}-%d").build()) {

  val isFailed = new AtomicBoolean(false)
  private val logger = LoggerFactory.getLogger(this.getClass)
  private var failureExc: Throwable = null

  /**
    * if lock is provided this class wraps runnable with lock
    * @param r Runnable to run wrapped
    * @param lock Lock
    */
  class RunnableWithLock(r: Runnable, lock: ReentrantLock) extends Runnable {
    override def run(): Unit = LockUtil.withLockOrDieDo[Unit](lock, (100, TimeUnit.SECONDS), Some(logger), () => r.run())
  }

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
      //throw throwable
    }
  }

  /**
    * submit task for execution with or without lock
    * @param runnable
    * @param l
    */
  def submit(runnable : Runnable, l : Option[ReentrantLock] = None) = {
    if(isShutdown)
      throw new IllegalStateException(s"Executor ${name} is no longer online. Unable to execute.")
    if (l.isDefined)
      super.execute(new RunnableWithLock(runnable, l.get))
    else
      super.execute(runnable)
  }

  override def submit(runnable: Runnable): Future[_] = {
    if(isShutdown)
      throw new IllegalStateException(s"Executor ${name} is no longer online. Unable to execute.")
    super.submit(runnable)
  }

  override def execute(runnable: Runnable) = {
    if(isShutdown)
      throw new IllegalStateException(s"Executor ${name} is no longer online. Unable to execute.")
    super.execute(runnable)
  }

  /**
    * get last exception
    * @return
    */
  def getException: Option[Throwable] = Option(failureExc)


  /**
    * safely shut down or die
    */

  def shutdownOrDie(amount: Long, tu: TimeUnit) = {
    this.shutdown()
    if(!this.awaitTermination(amount, tu))
      throw new IllegalStateException(s"Executor service with ${name} was unable to shut down in ${amount} ${tu}")
  }
}
