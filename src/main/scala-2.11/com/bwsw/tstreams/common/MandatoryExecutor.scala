package com.bwsw.tstreams.common

import java.util.concurrent.{Executors, Phaser}
import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreams.common.MandatoryExecutor.MandatoryExecutorException

import scala.collection.mutable

/**
  *
  */
class MandatoryExecutor {
  private val executorService = Executors.newSingleThreadScheduledExecutor()
  private val lock = new ReentrantLock(true)
  private val awaitSignalVar = new Phaser(1)
  private val queue = mutable.Queue[Runnable]()
  private var isReady = true

  private def tryGetTaskAndExecute() : Unit = {
    lock.lock()
    if (queue.nonEmpty) {
      val finishedFut = executorService.submit(new CallbackTask(queue.dequeue(), tryGetTaskAndExecute))
      try {
        finishedFut.get()
      }
      catch {
        case e : Exception =>
          throw new MandatoryExecutorException(s"Runnable failure: ${e.getMessage}")
      }
    }
    else {
      isReady = true
      awaitSignalVar.arriveAndDeregister()
      awaitSignalVar.register()
    }
    lock.unlock()
  }

  /**
    *
    * @param task
    */
  def submit(task : Runnable) = {
    lock.lock()
    if (isReady){
      queue.enqueue(task)
      tryGetTaskAndExecute()
      isReady = false
    } else {
      queue.enqueue(task)
    }
    lock.unlock()
  }

  /**
    *
    * @return
    */
  def await() = {
    awaitSignalVar.arriveAndAwaitAdvance()
  }
}

object MandatoryExecutor {
  class MandatoryExecutorException(msg : String) extends Exception(msg)
}

/**
  *
  * @param task
  * @param callback
  */
sealed class CallbackTask(private val task: Runnable, private val callback: () => Unit)
  extends Runnable {
  def run() {
    task.run()
    callback()
  }
}



