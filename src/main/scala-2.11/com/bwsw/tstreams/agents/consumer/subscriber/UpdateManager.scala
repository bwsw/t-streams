package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable.ListBuffer


class UpdateManager {
  private val lock = new ReentrantLock(true)
  private val executorWithRunnable = ListBuffer[(Executor, Runnable)]()
  private var updateThread : Thread = null
  private val isUpdating = new AtomicBoolean(false)

  def addExecutorWithRunnable(e : Executor, r : Runnable) = {
    lock.lock()
    executorWithRunnable += ((e, r))
    lock.unlock()
  }

  def startUpdate(updateInterval : Int) = {
    isUpdating.set(true)
    updateThread = new Thread(new Runnable {
      override def run(): Unit = {
        while(isUpdating.get()){
          lock.lock()
          executorWithRunnable.foreach{case(e,r)=>e.execute(r)}
          lock.unlock()
          Thread.sleep(updateInterval)
        }
      }
    })
    updateThread.start()
  }

  def stopUpdate() = {
    if (updateThread != null) {
      isUpdating.set(false)
      updateThread.join()
    }
  }
}
