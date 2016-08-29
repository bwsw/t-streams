package com.bwsw.tstreams.coordination.producer

import java.util.concurrent.TimeUnit

import com.bwsw.tstreams.common.FirstFailLockableTaskExecutor

/**
  * Created by Ivan Kudryavtsev on 29.08.16.
  * Incapsulates internal executor relationships and usage logic
  */
class ExecutorGraph(publisherThreadsCount: Int = 1, name: String = "") {

  val SHUTDOWN_TIMEOUT = 100
  val SHUTDOWN_UNITS = TimeUnit.SECONDS

  val general         = new FirstFailLockableTaskExecutor(s"ExecutorGraph-general-${name}")
  val newTransaction  = new FirstFailLockableTaskExecutor(s"ExecutorGraph-newTransaction-${name}")
  val cassandra     = new FirstFailLockableTaskExecutor(s"ExecutorGraph-cassandra-${name}")
  val materialize   = new FirstFailLockableTaskExecutor(s"ExecutorGraph-materialize-${name}")
  val publish       = new FirstFailLockableTaskExecutor(s"ExecutorGraph-publish-${name}", publisherThreadsCount)

  /**
    * Closes all
    */
  def shutdown() = this.synchronized {
    List(general, newTransaction, cassandra, materialize, publish)
      .foreach(e => e.shutdownOrDie(SHUTDOWN_TIMEOUT, SHUTDOWN_UNITS))
  }

  /**
    * submits task for execution
    * @param ex
    * @param f
    */
  private def submitTo(ex: FirstFailLockableTaskExecutor, f: () => Unit) = this.synchronized {
    List(general, newTransaction, cassandra, materialize, publish)
      .foreach(e => if(e.isFailed.get()) throw new IllegalStateException(s"Executor ${e.toString} is failed. No new tasks are allowed."))
    List(general, newTransaction, cassandra, materialize, publish)
      .foreach(e => if(ex == e) f())
  }

  def submitToGeneral(f: () => Unit)        = submitTo(general, f)
  def submitToNewTransaction(f: () => Unit) = submitTo(newTransaction, f)
  def submitToCassandra(f: () => Unit)    = submitTo(cassandra, f)
  def submitToMaterialize(f: () => Unit)  = submitTo(materialize, f)
  def submitToPublish(f: () => Unit)      = submitTo(publish, f)

}
