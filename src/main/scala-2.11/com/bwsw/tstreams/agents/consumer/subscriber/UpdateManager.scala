package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable.ListBuffer

//TODO use actor approach
/**
 * Manager for maintain async updates on [[TransactionsBuffer]]]
 */
class UpdateManager {
  private val lockExecutors = new ReentrantLock(true)
  private val executorWithRunnable = ListBuffer[(Executor, Runnable)]()
  private var updateThread : Thread = null
  private val isUpdating = new AtomicBoolean(false)

  /**
   * Add executor with runnable to list
   * @param e Executor ref
   * @param r Runnable ref
   */
  def addExecutorWithRunnable(e : Executor, r : Runnable) = {
    lockExecutors.lock()
    executorWithRunnable += ((e, r))
    lockExecutors.unlock()
  }

  /**
   * Start executing all runnables with their executor
   * with concrete updateInterval
   *
   * @param updateInterval Delay between runnables execution
   */
  def startUpdate(updateInterval : Int) = {
    isUpdating.set(true)
    updateThread = new Thread(new Runnable {
      override def run(): Unit = {
        while(isUpdating.get()){
          lockExecutors.lock()
          executorWithRunnable.foreach{case(e,r)=>e.execute(r)}
          lockExecutors.unlock()
          Thread.sleep(updateInterval)
        }
      }
    })
    updateThread.start()
  }

  /**
   * Stop updates
   */
  def stopUpdate() = {
    if (updateThread != null) {
      isUpdating.set(false)
      updateThread.join()
      executorWithRunnable.clear()
    }
  }
}
