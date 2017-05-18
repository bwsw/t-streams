package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreams.agents.group.{CheckpointInfo, GroupParticipant}
import com.bwsw.tstreams.common.{Functions, GeneralOptions}
import com.bwsw.tstreams.coordination.server.StateUpdateServer
import com.bwsw.tstreams.storage.StorageClient
import com.bwsw.tstreams.streams.Stream
import org.slf4j.LoggerFactory

import scala.collection.mutable

object Subscriber {
  val logger = LoggerFactory.getLogger(this.getClass)
  var SHUTDOWN_WAIT_MAX_SECONDS = GeneralOptions.SHUTDOWN_WAIT_MAX_SECONDS
}

/**
  * Created by Ivan Kudryavtsev on 19.08.16.
  * Class implements subscriber
  */
class Subscriber(val name: String,
                 val stream: Stream,
                 val options: SubscriberOptions,
                 val callback: Callback) extends GroupParticipant {

  private val transactionsBufferWorkers = mutable.Map[Int, TransactionBufferWorker]()
  private val processingEngines = mutable.Map[Int, ProcessingEngine]()

  private val bufferWorkerThreads = calculateBufferWorkersThreadAmount()
  private val peWorkerThreads = calculateProcessingEngineWorkersThreadAmount()

  val l = options.agentAddress.split(":")
  val host = l.head
  val port = l.tail.head
  private var stateUpdateServer: StateUpdateServer = null
  private val consumer = new com.bwsw.tstreams.agents.consumer.Consumer(
    name,
    stream,
    options.getConsumerOptions())

  private val isStarted = new AtomicBoolean(false)
  private val isStopped = new AtomicBoolean(false)

  private val coordinator = new Coordinator()

  /**
    * Erathosphene's grating algorithm
    *
    * @param parts
    * @param thID
    * @param total
    * @return
    */
  private def distributeBetweenWorkerThreads(parts: Set[Int], thID: Int, total: Int): Set[Int] = {
    val array = parts.toArray.sorted
    val set = mutable.Set[Int]()
    for (i <- thID until array.length by total)
      set.add(array(i))
    set.toSet
  }

  val transactionsBuffers = mutable.Map[Int, TransactionBuffer]()

  def dumpCounters() = {
    transactionsBuffers.foreach(kv => kv._2.counters.dump(kv._1))
  }

  /**
    * Starts the subscriber
    */
  def start() = {

    val usedPartitionsSet = options.readPolicy.getUsedPartitions

    Subscriber.logger.info(s"[INIT] Subscriber $name: BEGIN INIT.")
    Subscriber.logger.info(s"[INIT] Subscriber $name: Address ${options.agentAddress}")
    Subscriber.logger.info(s"[INIT] Subscriber $name: Partitions $usedPartitionsSet")

    if (isStopped.get())
      throw new IllegalStateException(s"Subscriber $name is stopped already. Start after stop is no longer possible.")

    if (isStarted.get())
      throw new IllegalStateException(s"Subscriber $name is started already. Double start is detected.")

    Subscriber.logger.info(s"[INIT] Subscriber $name: Consumer $name is about to start for subscriber.")

    consumer.start()

    Subscriber.logger.info(s"[INIT] Subscriber $name: Consumer $name has been started.")

    /**
      * Initialize processing engines
      */


    Subscriber.logger.info(s"[INIT] Subscriber $name: Is about to create PEs.")

    for (thID <- 0 until peWorkerThreads) {
      val parts: Set[Int] = distributeBetweenWorkerThreads(usedPartitionsSet, thID, peWorkerThreads)

      Subscriber.logger.info(s"[INIT] Subscriber $name PE $thID got $parts")

      processingEngines(thID) = new ProcessingEngine(consumer, parts, options.transactionsQueueBuilder,
        callback, options.pollingFrequencyDelayMs)
    }

    Subscriber.logger.info(s"[INIT] Subscriber $name: has created PEs.")

    /**
      * end initialize
      */

    transactionsBuffers.clear()

    Subscriber.logger.info(s"[INIT] Subscriber $name: Is about to create Transaction Buffers.")

    options.readPolicy.getUsedPartitions foreach (part =>
      for (thID <- 0 until peWorkerThreads) {
        if (part % peWorkerThreads == thID) {
          transactionsBuffers(part) = new TransactionBuffer(processingEngines(thID).getQueue(), options.transactionQueueMaxLengthThreshold)
          Subscriber.logger.info(s"[INIT] Subscriber $name: TransactionBuffer $part is bound to PE $thID.")
        }
      })

    Subscriber.logger.info(s"[INIT] Subscriber $name: has created Transaction Buffers.")
    Subscriber.logger.info(s"[INIT] Subscriber $name: Is about to create TransactionBufferWorkers.")

    for (thID <- 0 until bufferWorkerThreads) {
      val worker = new TransactionBufferWorker()

      options.readPolicy.getUsedPartitions foreach (part =>
        if (part % bufferWorkerThreads == thID) {
          worker.assign(part, transactionsBuffers(part))
          Subscriber.logger.info(s"[INIT] Subscriber $name: TransactionBufferWorker $thID is bound to TransactionBuffer $part.")
        })

      transactionsBufferWorkers(thID) = worker
    }

    Subscriber.logger.info(s"[INIT] Subscriber $name: has created TransactionBufferWorkers.")
    Subscriber.logger.info(s"[INIT] Subscriber $name: is about to launch the coordinator.")

    coordinator.bootstrap(
      agentAddress = options.agentAddress,
      stream = stream.name,
      partitions = Set[Int]().empty ++ options.readPolicy.getUsedPartitions,
      zkRootPath = options.zkRootPath,
      zkHosts = options.zkHosts,
      zkConnectionTimeoutMs = options.zkConnectionTimeoutMs,
      zkSessionTimeoutMs = options.zkSessionTimeoutMs,
      zkRetryDelayMs = options.zkRetryDelayMs,
      zkRetryCount = options.zkRetryCount)

    Subscriber.logger.info(s"[INIT] Subscriber $name: has launched the coordinator.")
    Subscriber.logger.info(s"[INIT] Subscriber $name: is about to launch the UDP server.")

    stateUpdateServer = new StateUpdateServer(host, Integer.parseInt(port), Runtime.getRuntime.availableProcessors(), transactionsBufferWorkers)
    stateUpdateServer.start()

    Subscriber.logger.info(s"[INIT] Subscriber $name: has launched the UDP server.")
    Subscriber.logger.info(s"[INIT] Subscriber $name: is about to launch Polling tasks to executors.")

    for (thID <- 0 until peWorkerThreads) {
      processingEngines(thID).start()
    }

    Subscriber.logger.info(s"[INIT] Subscriber $name: has launched Polling tasks to executors.")
    Subscriber.logger.info(s"[INIT] Subscriber $name: END INIT.")

    isStarted.set(true)

  }

  /**
    *
    */
  def stop() = {

    if (isStopped.getAndSet(true))
      throw new IllegalStateException(s"Subscriber $name is stopped already. Double stop is impossible.")

    if (!isStarted.getAndSet(false))
      throw new IllegalStateException(s"Subscriber $name is not started yet. Stop is impossible.")

    stateUpdateServer.stop()

    processingEngines.foreach(kv => kv._2.stop())
    processingEngines.clear()
    transactionsBufferWorkers.foreach(kv => kv._2.stop())
    transactionsBufferWorkers.clear()
    coordinator.shutdown()
    consumer.stop()
  }

  /**
    * Calculates amount of BufferWorkers threads based on user requested amount and total partitions amount.
    *
    * @return
    */
  private def calculateBufferWorkersThreadAmount(): Int = {
    val maxThreads = options.readPolicy.getUsedPartitions.size
    val minThreads = options.transactionBufferWorkersThreadPoolAmount
    Functions.calculateThreadAmount(minThreads, maxThreads)
  }

  /**
    * Calculates amount of Processing Engine workers
    *
    * @return
    */
  def calculateProcessingEngineWorkersThreadAmount(): Int = {
    val maxThreads = options.readPolicy.getUsedPartitions.size
    val minThreads = options.processingEngineWorkersThreadAmount
    Functions.calculateThreadAmount(minThreads, maxThreads)
  }


  /**
    * Returns consumer inner object
    *
    * @return
    */
  def getConsumer = consumer

  override private[tstreams] def getAgentName(): String = consumer.getAgentName()

  /**
    * Info to commit
    */
  override private[tstreams] def getCheckpointInfoAndClear(): List[CheckpointInfo] = consumer.getCheckpointInfoAndClear()

  override private[tstreams] def getStorageClient(): StorageClient = consumer.getStorageClient()

}

