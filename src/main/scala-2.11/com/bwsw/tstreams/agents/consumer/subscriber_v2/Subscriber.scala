package com.bwsw.tstreams.agents.consumer.subscriber_v2

import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreams.common.FirstFailLockableTaskExecutor
import com.bwsw.tstreams.streams.TStream
import org.slf4j.LoggerFactory

import scala.collection.mutable

object Subscriber {
  val logger = LoggerFactory.getLogger(this.getClass)
}

/**
  * Created by Ivan Kudryavtsev on 19.08.16.
  * Class implements subscriber
  */
class Subscriber[T](val name: String,
                    val stream: TStream[Array[Byte]],
                    val options: Options[T],
                    val callback: Callback[T]) {

  val txnBufferWorkers = mutable.Map[Int, TransactionBufferWorker]()
  val processingEngines = mutable.Map[Int, ProcessingEngine[T]]()

  val bufferWorkerThreads = calculateBufferWorkersThreadAmount()
  val peWorkerThreads     = calculateProcessingEngineWorkersThreadAmount()

  val consumer = new com.bwsw.tstreams.agents.consumer.Consumer[T](
      name,
      stream,
      options.getConsumerOptions())

  val isStarted = new AtomicBoolean(false)

  /**
    *  Starts the subscriber
    */
  def start() = this.synchronized {

    if(isStarted.getAndSet(true))
      throw new IllegalStateException("Double start is detected. Please stop it first.")

    val txnBuffers = mutable.Map[Int, TransactionBuffer]()

    /**
      * Initialize processing engines
      */

    for(thID <- 0 until peWorkerThreads) {
      val parts: Set[Int] = Set[Int]().empty ++ options.readPolicy.getUsedPartitions() filter (p => p % thID == 0)
      processingEngines(thID) = new ProcessingEngine[T](consumer, parts, options.txnQueueBuilder, callback)
    }

    /**
      * end initialize
      */

    txnBuffers.clear()

    options.readPolicy.getUsedPartitions() foreach (part =>
      for(thID <- 0 until peWorkerThreads) {
        if(part % peWorkerThreads == thID)
          txnBuffers(part) = new TransactionBuffer(processingEngines(thID).getQueue())
      })



    for(thID <- 0 until bufferWorkerThreads) {
      val worker = new TransactionBufferWorker()

      options.readPolicy.getUsedPartitions() foreach (part =>
        if(part % bufferWorkerThreads == thID)
          worker.assign(part, txnBuffers(part)))

      txnBufferWorkers(thID) = worker
    }

    consumer.start()

    for(thID <- 0 until peWorkerThreads) {
      processingEngines(thID).getExecutor().submit(new Poller[T](processingEngines(thID), options.pollingFrequencyDelay))
    }

  }

  /**
    *
    */
  def stop() = {
    if(!isStarted.getAndSet(false))
      throw new IllegalStateException("Double stop is detected. Please start it first.")

    processingEngines foreach(kv => kv._2.stop())
    processingEngines clear()

    txnBufferWorkers foreach (kv => kv._2.stop())
    txnBufferWorkers.clear()

    consumer.stop()
  }

  /**
    * Calculates amount of BufferWorkers threads based on user requested amount and total partitions amount.
    *
    * @return
    */
  private def calculateBufferWorkersThreadAmount(): Int = {
    val maxThreads = options.readPolicy.getUsedPartitions().size
    val minThreads = options.txnBufferWorkersThreadPoolAmount
    calculateThreadAmount(minThreads, maxThreads)
  }

  /**
    * Calculates amount of Processing Engine workers
    *
    * @return
    */
  def calculateProcessingEngineWorkersThreadAmount(): Int = {
    val maxThreads = options.readPolicy.getUsedPartitions().size
    val minThreads = options.processingEngineWorkersThreadAmount
    calculateThreadAmount(minThreads, maxThreads)
  }

  private def calculateThreadAmount(minThreads: Int, maxThreads: Int): Int = {
    if (minThreads >= maxThreads) {
      Subscriber.logger.warn(s"User requested ${minThreads} worker threads, but total partitions amount is ${maxThreads}. Will use ${maxThreads}")
      return maxThreads
    }

    if(minThreads <= 0) {
      Subscriber.logger.warn(s"User requested ${minThreads} worker threads, but minimal amount is 1. Will use 1 worker thread.")
      return 1
    }

    if(maxThreads % minThreads == 0) {
      return minThreads
    }

    for(i <- minThreads to maxThreads) {
      if (maxThreads % i == 0)
        return i
    }
    return maxThreads
  }


}