package com.bwsw.tstreams.agents.producer

import java.util.concurrent.TimeUnit

import com.bwsw.tstreams.common.FirstFailLockableTaskExecutor

/**
  * Created by Ivan Kudryavtsev on 29.08.16.
  * Encapsulates internal executor relationships and usage logic
  */
class ExecutorGraph(name: String = "", publisherThreadsAmount: Int = 1) {

  val SHUTDOWN_UNITS = TimeUnit.SECONDS

  val general = new FirstFailLockableTaskExecutor(s"ExecutorGraph-general-$name")
  val newTransaction = new FirstFailLockableTaskExecutor(s"ExecutorGraph-newTransaction-$name")
  val materialize = new FirstFailLockableTaskExecutor(s"ExecutorGraph-materialize-$name")
  val publish = new FirstFailLockableTaskExecutor(s"ExecutorGraph-publish-$name", publisherThreadsAmount)

  /**
    * Closes all
    */
  def shutdown() = this.synchronized {
    List(general, newTransaction, materialize, publish)
      .foreach(e => e.shutdownOrDie(Producer.SHUTDOWN_WAIT_MAX_SECONDS, SHUTDOWN_UNITS))
  }

  /**
    * submits task for execution
    *
    * @param ex
    * @param f
    */
  private def submitTo(name: String, ex: FirstFailLockableTaskExecutor, f: () => Unit) = this.synchronized {
    List(general, newTransaction, materialize, publish)
      .foreach(e => if (e.isFailed.get()) throw new IllegalStateException(s"Executor ${e.toString} is failed. No new tasks are allowed."))

    ex.submit(s"<ExecutorGraphTask> :: $name", new Runnable { override def run(): Unit = f() })

  }

  def submitToGeneral(name: String, f: () => Unit) = submitTo(name, general, f)
  def submitToNewTransaction(name: String, f: () => Unit) = submitTo(name, newTransaction, f)
  def submitToMaterialize(name: String, f: () => Unit) = submitTo(name, materialize, f)
  def submitToPublish(name: String, f: () => Unit) = submitTo(name, publish, f)

}
