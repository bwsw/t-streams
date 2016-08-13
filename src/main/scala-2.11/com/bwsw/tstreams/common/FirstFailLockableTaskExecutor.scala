package com.bwsw.tstreams.common

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent._

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

  class RunnableWithLock(r: Runnable, lock: ReentrantLock) extends Runnable {
    override def run(): Unit = LockUtil.withLockOrDieDo[Unit](lock, (100, TimeUnit.SECONDS), Some(logger), () => r.run())

    def getLock: ReentrantLock = lock
  }

  override def afterExecute(runnable: Runnable, throwable: Throwable): Unit =  {
    super.afterExecute(runnable, throwable)
    if(throwable != null) {
      if(runnable.isInstanceOf[RunnableWithLock])
        runnable.asInstanceOf[RunnableWithLock].getLock.unlock()
      this.shutdownNow()
      failureExc = throwable
      isFailed.set(true)
      throwable.getStackTrace.foreach(ste => logger.error(ste.toString))
    }
  }

  def submit(runnable : Runnable, l : Option[ReentrantLock] = None) = {
    if (l.isDefined)
      super.execute(new RunnableWithLock(runnable, l.get))
    else
      super.execute(runnable)
  }

  def getException: Option[Throwable] = Option(failureExc)

  def shutdownSafe() : Unit = {
    shutdown()
  }
}
