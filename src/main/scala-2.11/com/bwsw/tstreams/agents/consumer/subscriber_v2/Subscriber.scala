package com.bwsw.tstreams.agents.consumer.subscriber_v2

import java.util.concurrent.atomic.AtomicBoolean

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

  val threads = calculateThreadAmount()
  val txnBufferWorkers = mutable.Map[Int, TransactionBufferWorker]()

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
    options.readPolicy.getUsedPartitions() foreach (part =>
      txnBuffers(part) = new TransactionBuffer(options.txnQueueBuilder.generateQueueObject(part)))

    txnBufferWorkers foreach (kv => kv._2.stop())
    txnBufferWorkers.clear()

    for(thID <- 0 until threads) {
      val worker = new TransactionBufferWorker()

      options.readPolicy.getUsedPartitions() foreach (part =>
        if(part % threads == thID)
          worker.assign(part, txnBuffers(part)))

      txnBufferWorkers(thID) = worker
    }

    consumer.start()
  }

  /**
    * Calculates amount of threads based on user requested amount and total partitions amount.
 *
    * @return
    */
  private def calculateThreadAmount(): Int = {
    val maxThreads = options.readPolicy.getUsedPartitions().size
    val minThreads = options.threadPoolAmount

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