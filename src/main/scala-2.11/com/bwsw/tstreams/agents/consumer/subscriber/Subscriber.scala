package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreams.common.Functions
import com.bwsw.tstreams.coordination.server.RequestsTcpServer
import com.bwsw.tstreams.streams.TStream
import org.slf4j.LoggerFactory

import scala.collection.mutable


/**
  * Created by Ivan Kudryavtsev on 19.08.16.
  * Class implements subscriber
  */
class Subscriber[T](val name: String,
                    val stream: TStream[Array[Byte]],
                    val options: Options[T],
                    val callback: Callback[T]) {

  private val txnBufferWorkers = mutable.Map[Int, TransactionBufferWorker]()
  private val processingEngines = mutable.Map[Int, ProcessingEngine[T]]()

  private val bufferWorkerThreads = calculateBufferWorkersThreadAmount()
  private val peWorkerThreads     = calculateProcessingEngineWorkersThreadAmount()

  val l = options.agentAddress.split(":")
  val host = l.head
  val port = l.tail.head
  private var tcpServer: RequestsTcpServer = null
  private val consumer = new com.bwsw.tstreams.agents.consumer.Consumer[T](
      name,
      stream,
      options.getConsumerOptions())

  private val isStarted = new AtomicBoolean(false)

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
    for(i <- thID until array.size by total)
      set.add(array(i))
    set.toSet
  }

  /**
    *  Starts the subscriber
    */
  def start() = {

    val usedPartitionsSet = options.readPolicy.getUsedPartitions().toSet

    Subscriber.logger.info(s"[INIT] Subscriber ${name}: BEGIN INIT.")
    Subscriber.logger.info(s"[INIT] Subscriber ${name}: Address ${options.agentAddress}")
    Subscriber.logger.info(s"[INIT] Subscriber ${name}: Partitions ${usedPartitionsSet}")

    if(isStarted.getAndSet(true))
      throw new IllegalStateException("Double start is detected. Please stop it first.")

    val txnBuffers = mutable.Map[Int, TransactionBuffer]()

    Subscriber.logger.info(s"[INIT] Subscriber ${name}: Consumer ${name} is about to start for subscriber.")

    consumer.start()

    Subscriber.logger.info(s"[INIT] Subscriber ${name}: Consumer ${name} has been started.")

    /**
      * Initialize processing engines
      */


    Subscriber.logger.info(s"[INIT] Subscriber ${name}: Is about to create PEs.")

    for(thID <- 0 until peWorkerThreads) {
      val parts: Set[Int] = distributeBetweenWorkerThreads(usedPartitionsSet, thID, peWorkerThreads)

      Subscriber.logger.info(s"[INIT] Subscriber ${name}: PE ${thID} got ${parts}")

      processingEngines(thID) = new ProcessingEngine[T](consumer, parts, options.txnQueueBuilder, callback)
    }

    Subscriber.logger.info(s"[INIT] Subscriber ${name}: has created PEs.")

    /**
      * end initialize
      */

    txnBuffers.clear()

    Subscriber.logger.info(s"[INIT] Subscriber ${name}: Is about to create Transaction Buffers.")

    options.readPolicy.getUsedPartitions() foreach (part =>
      for(thID <- 0 until peWorkerThreads) {
        if(part % peWorkerThreads == thID) {
          txnBuffers(part) = new TransactionBuffer(processingEngines(thID).getQueue())
          Subscriber.logger.info(s"[INIT] Subscriber ${name}: TransactionBuffer ${part} is bound to PE ${thID}.")
        }
      })

    Subscriber.logger.info(s"[INIT] Subscriber ${name}: has created Transaction Buffers.")
    Subscriber.logger.info(s"[INIT] Subscriber ${name}: Is about to create TransactionBufferWorkers.")

    for(thID <- 0 until bufferWorkerThreads) {
      val worker = new TransactionBufferWorker()

      options.readPolicy.getUsedPartitions() foreach (part =>
        if(part % bufferWorkerThreads == thID) {
          worker.assign(part, txnBuffers(part))
          Subscriber.logger.info(s"[INIT] Subscriber ${name}: TransactionBufferWorker ${thID} is bound to TransactionBuffer ${part}.")
        })

      txnBufferWorkers(thID) = worker
    }

    Subscriber.logger.info(s"[INIT] Subscriber ${name}: has created TransactionBufferWorkers.")
    Subscriber.logger.info(s"[INIT] Subscriber ${name}: is about to launch the coordinator.")

    coordinator.bootstrap(
      agentAddress = options.agentAddress,
      stream = stream.getName,
      partitions = Set[Int]().empty ++ options.readPolicy.getUsedPartitions(),
      zkRootPath = options.zkRootPath,
      zkHosts = options.zkHosts,
      zkConnectionTimeout = options.zkConnectionTimeout,
      zkSessionTimeout = options.zkSessionTimeout)

    Subscriber.logger.info(s"[INIT] Subscriber ${name}: has launched the coordinator.")
    Subscriber.logger.info(s"[INIT] Subscriber ${name}: is about to launch the tcp server.")

    tcpServer = new RequestsTcpServer(host, Integer.parseInt(port), new TransactionStateMessageChannelHandler(txnBufferWorkers))
    tcpServer.start()

    Subscriber.logger.info(s"[INIT] Subscriber ${name}: has launched the tcp server.")
    Subscriber.logger.info(s"[INIT] Subscriber ${name}: is about to launch Polling tasks to executors.")

    for(thID <- 0 until peWorkerThreads) {
      processingEngines(thID).getExecutor().submit(s"<Poller ${processingEngines(thID)}>", new Poller[T](processingEngines(thID), options.pollingFrequencyDelay))
    }

    Subscriber.logger.info(s"[INIT] Subscriber ${name}: has launched Polling tasks to executors.")
    Subscriber.logger.info(s"[INIT] Subscriber ${name}: END INIT.")

  }

  /**
    *
    */
  def stop() = {
    if(!isStarted.getAndSet(false))
      throw new IllegalStateException("Double stop is detected. Please start it first.")

    processingEngines.foreach(kv => kv._2.stop())
    processingEngines.clear()
    txnBufferWorkers.foreach (kv => kv._2.stop())
    txnBufferWorkers.clear()

    tcpServer.stop()
    coordinator.shutdown()
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
    Functions.calculateThreadAmount(minThreads, maxThreads)
  }

  /**
    * Calculates amount of Processing Engine workers
    *
    * @return
    */
  def calculateProcessingEngineWorkersThreadAmount(): Int = {
    val maxThreads = options.readPolicy.getUsedPartitions().size
    val minThreads = options.processingEngineWorkersThreadAmount
    Functions.calculateThreadAmount(minThreads, maxThreads)
  }



  /**
    * Returns consumer inner object
    *
    * @return
    */
  def getConsumer() = consumer

}

object Subscriber {
  val logger = LoggerFactory.getLogger(this.getClass)
}