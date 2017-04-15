package com.bwsw.tstreams.agents.producer

import java.util.concurrent.TimeUnit

import com.bwsw.tstreams.common.FirstFailLockableTaskExecutor

/**
  * Created by Ivan Kudryavtsev on 29.08.16.
  * Encapsulates internal executor relationships and usage logic
  */
class ExecutorGraph(name: String = "") {

  val SHUTDOWN_UNITS = TimeUnit.SECONDS
  val newTransaction = new FirstFailLockableTaskExecutor(s"ExecutorGraph-newTransaction-$name")

  /**
    * Closes all
    */
  def shutdown() = this.synchronized {
    List(newTransaction)
      .foreach(e => e.shutdownOrDie(Producer.SHUTDOWN_WAIT_MAX_SECONDS, SHUTDOWN_UNITS))
  }

  /**
    * submits task for execution
    *
    * @param ex
    * @param f
    */
  private def submitTo(name: String, ex: FirstFailLockableTaskExecutor, f: () => Unit) = this.synchronized {
    List(newTransaction)
      .foreach(e => if (e.isFailed.get()) throw new IllegalStateException(s"Executor ${e.toString} is failed. No new tasks are allowed."))

    ex.submit(s"<ExecutorGraphTask> :: $name", () => f())

  }

  def submitToNewTransaction(name: String, f: () => Unit) = submitTo(name, newTransaction, f)


}
