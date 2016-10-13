package com.bwsw.tstreams.agents.producer

import java.util.concurrent.TimeUnit

import com.bwsw.tstreams.common.FirstFailLockableTaskExecutor

/**
  * Created by Ivan Kudryavtsev on 29.08.16.
  * Encapsulates internal executor relationships and usage logic
  */
class ExecutorGraph(name: String = "", publisherThreadsAmount: Int = 1) {

  val SHUTDOWN_TIMEOUT = 100
  val SHUTDOWN_UNITS = TimeUnit.SECONDS

  val general = new FirstFailLockableTaskExecutor(s"ExecutorGraph-general-$name")
  val newTransaction = new FirstFailLockableTaskExecutor(s"ExecutorGraph-newTransaction-$name")
  val cassandra = new FirstFailLockableTaskExecutor(s"ExecutorGraph-cassandra-$name")
  val materialize = new FirstFailLockableTaskExecutor(s"ExecutorGraph-materialize-$name")
  val publish = new FirstFailLockableTaskExecutor(s"ExecutorGraph-publish-$name", publisherThreadsAmount)

  /**
    * Closes all
    */
  def shutdown() = this.synchronized {
    List(general, newTransaction, cassandra, materialize, publish)
      .foreach(e => e.shutdownOrDie(SHUTDOWN_TIMEOUT, SHUTDOWN_UNITS))
  }

  /**
    * submits task for execution
    *
    * @param ex
    * @param f
    */
  private def submitTo(name: String, ex: FirstFailLockableTaskExecutor, f: () => Unit) = this.synchronized {
    List(general, newTransaction, cassandra, materialize, publish)
      .foreach(e => if (e.isFailed.get()) throw new IllegalStateException(s"Executor ${e.toString} is failed. No new tasks are allowed."))
    List(general, newTransaction, cassandra, materialize, publish)
      .foreach(e => if (ex == e) e.submit(s"<ExecutorGraphTask> :: $name", new Runnable {
        override def run(): Unit = f()
      }))
  }

  def submitToGeneral(name: String, f: () => Unit) = submitTo(name, general, f)

  def submitToNewTransaction(name: String, f: () => Unit) = submitTo(name, newTransaction, f)

  def submitToCassandra(name: String, f: () => Unit) = submitTo(name, cassandra, f)

  def submitToMaterialize(name: String, f: () => Unit) = submitTo(name, materialize, f)

  def submitToPublish(name: String, f: () => Unit) = submitTo(name, publish, f)

  def getCassandra() = cassandra

}
