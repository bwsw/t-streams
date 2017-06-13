/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.bwsw.tstreams.common

import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.locks.ReentrantLock

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.slf4j.LoggerFactory

/**
  * Implements the executor which analyzes task exception, unlocks if necessary and
  * stops further execution immediately if task is failed.
  *
  * @param name
  */
private[tstreams] class FirstFailLockableTaskExecutor(name: String, cnt: Int = 1)
  extends ThreadPoolExecutor(cnt, cnt, 0, TimeUnit.MILLISECONDS,
    new LinkedBlockingQueue[Runnable],
    new ThreadFactoryBuilder().setNameFormat(s"$name-%d").build()) {

  private val queueLengthThreshold = new AtomicInteger(100)
  private val taskFullDelayThresholdMs = new AtomicInteger(120)
  private val taskDelayThresholdMs = new AtomicInteger(100)
  private val taskRunDelayThresholdMs = new AtomicInteger(20)

  queueLengthThreshold.set(FirstFailLockableExecutor.queueLengthThreshold.get)
  taskFullDelayThresholdMs.set(FirstFailLockableExecutor.taskFullDelayThresholdMs.get)
  taskDelayThresholdMs.set(FirstFailLockableExecutor.taskDelayThresholdMs.get)
  taskRunDelayThresholdMs.set(FirstFailLockableExecutor.taskRunDelayThresholdMs.get)

  /**
    * Allows to define thresholds in custom way
    *
    * @param queueLengthThreshold
    * @param taskFullDelayThresholdMs
    * @param taskDelayThresholdMs
    * @param taskRunDelayThresholdMs
    */
  def setThresholds(queueLengthThreshold: Int,
                    taskFullDelayThresholdMs: Int,
                    taskDelayThresholdMs: Int,
                    taskRunDelayThresholdMs: Int): Unit = {
    this.queueLengthThreshold.set(queueLengthThreshold)
    this.taskFullDelayThresholdMs.set(taskFullDelayThresholdMs)
    this.taskDelayThresholdMs.set(taskDelayThresholdMs)
    this.taskRunDelayThresholdMs.set(taskRunDelayThresholdMs)
  }

  private val isFailed = new AtomicBoolean(false)
  private val logger = LoggerFactory.getLogger(this.getClass)
  private var failureExc: Throwable = null


  private[tstreams] abstract class FirstFailExecutorRunnable extends Runnable {
    val submitTime = System.currentTimeMillis()
    var runBeginTime = 0L
  }

  /**
    * if lock is provided this class wraps runnable with lock
    *
    * @param r    Runnable to run wrapped
    * @param lock Lock
    */
  private[tstreams] class RunnableWithLock(r: Runnable, lock: ReentrantLock, name: String) extends FirstFailExecutorRunnable {

    override def toString() = name

    override def run(): Unit = {
      runBeginTime = System.currentTimeMillis()
      LockUtil.withLockOrDieDo[Unit](lock, (100, TimeUnit.SECONDS), Some(logger), () => r.run())
    }
  }

  private[tstreams] class RunnableWithoutLock(r: Runnable, name: String) extends FirstFailExecutorRunnable {

    override def toString() = name

    override def run(): Unit = {
      try {
        runBeginTime = System.currentTimeMillis()
        r.run()
      } catch {
        case e: Exception =>
          e.printStackTrace()
          throw e
      }
    }
  }

  /**
    * Allows to determine that task was executed with errors and prevents further task to be executed
    *
    * @param runnable
    * @param throwable
    */
  override def afterExecute(runnable: Runnable, throwable: Throwable): Unit = {
    super.afterExecute(runnable, throwable)

    try {
      val myRunnable = runnable.asInstanceOf[FirstFailExecutorRunnable]

      val now = System.currentTimeMillis()
      val thresholdFull = taskFullDelayThresholdMs.get()
      val threshold = taskDelayThresholdMs.get()
      val thresholdRun = taskRunDelayThresholdMs.get()

      if (now - myRunnable.submitTime > thresholdFull) {
        logger.debug(s"Task $myRunnable has delayed Full in executor $name for ${now - myRunnable.submitTime} msecs. Threshold is: $thresholdFull msecs.")
      }

      if (myRunnable.runBeginTime - myRunnable.submitTime > threshold) {
        logger.debug(s"Task $myRunnable has delayed in Queue before run in executor $name for ${myRunnable.runBeginTime - myRunnable.submitTime} msecs. Threshold is: $threshold msecs.")
      }

      if (now - myRunnable.runBeginTime > thresholdRun) {
        logger.debug(s"Task $myRunnable has run in executor $name for ${now - myRunnable.runBeginTime} msecs. Threshold is: $thresholdRun msecs.")
      }

    } catch {
      case e: ClassCastException =>
    }

    if (throwable != null) {
      this.shutdownNow()
      failureExc = throwable
      isFailed.set(true)
      throwable.getStackTrace.foreach(ste => logger.error(ste.toString))
      //throw throwable
    }
  }

  private def notifyQueueTooLong() = {
    val qSize = this.getQueue.size()
    if (qSize > queueLengthThreshold.get())
      logger.debug(s"Executor $name achieved queue length $qSize, threshold is ${queueLengthThreshold.get()}")
  }

  /**
    * submit task for execution with or without lock
    *
    * @param runnable
    * @param l
    */
  def submit(name: String, runnable: Runnable, l: Option[ReentrantLock] = None): Future[_] = {
    if (isShutdown)
      throw new IllegalStateException(s"Executor $name is no longer online. Unable to execute.")

    notifyQueueTooLong()

    if (l.isDefined)
      super.submit(new RunnableWithLock(runnable, l.get, name))
    else
      super.submit(new RunnableWithoutLock(runnable, name))
  }

  def submit[T](name: String, runnable: Runnable, l: Option[ReentrantLock], result: T): Future[T] = {
    if (isShutdown)
      throw new IllegalStateException(s"Executor $name is no longer online. Unable to execute.")

    notifyQueueTooLong()

    if (l.isDefined)
      super.submit(new RunnableWithLock(runnable, l.get, name), result)
    else
      super.submit(new RunnableWithoutLock(runnable, name), result)
  }

  /**
    * get last exception
    *
    * @return
    */
  def getException: Option[Throwable] = Option(failureExc)


  /**
    * safely shut down or die
    */

  def shutdownOrDie(amount: Long, tu: TimeUnit) = {
    this.shutdown()
    if (!this.awaitTermination(amount, tu))
      throw new IllegalStateException(s"Executor service with $name was unable to shut down in $amount $tu")
  }
}

object FirstFailLockableExecutor {
  val queueLengthThreshold = new AtomicInteger(100)
  val taskFullDelayThresholdMs = new AtomicInteger(120)
  val taskDelayThresholdMs = new AtomicInteger(100)
  val taskRunDelayThresholdMs = new AtomicInteger(20)
}