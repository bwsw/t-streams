/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreams.agents.consumer.TransactionOperator
import com.bwsw.tstreams.agents.consumer.subscriber.QueueBuilder.QueueType
import com.bwsw.tstreams.common.FirstFailLockableTaskExecutor
import com.bwsw.tstreamstransactionserver.rpc.{TransactionState, TransactionStates}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.util.{Failure, Random, Try}

/**
  * Created by Ivan Kudryavtsev on 20.08.16.
  * Does top-level management tasks for new events.
  */
private[tstreams] class ProcessingEngine(consumer: TransactionOperator,
                                         partitions: Set[Int],
                                         queueBuilder: QueueBuilder.Abstract,
                                         callback: Callback,
                                         pollingInterval: Int,
                                         authKey: String) {

  private val id = Math.abs(Random.nextInt())
  // keeps last transaction states processed
  private val lastTransactionsMap = mutable.Map[Int, TransactionState]()
  private val lastPartitionsEventsMap = mutable.Map[Int, Long]()

  private val isRunning = new AtomicBoolean(false)

  private val executor = new Thread(() => {
    while (isRunning.get)
      processReadyTransactions(pollingInterval)
  }, s"pe-$id-executor")

  private val loadExecutor = new FirstFailLockableTaskExecutor(s"pe-$id-loadExecutor")

  val isThresholdsSet = new AtomicBoolean(false)

  private val queue = queueBuilder.generateQueueObject(Math.abs(Random.nextInt()))
  private var isFirstTime = true


  ProcessingEngine.logger.info(s"Processing engine $id will serve $partitions.")

  def getQueue(): QueueType = queue

  def getLastPartitionActivity(partition: Int) = lastPartitionsEventsMap(partition)

  def setLastPartitionActivity(partition: Int): Unit = {
    lastPartitionsEventsMap(partition) = System.currentTimeMillis()
  }

  def getLastTransactionHandled(partition: Int) = lastTransactionsMap(partition)

  // loaders
  val fastLoader = new TransactionFastLoader(partitions, lastTransactionsMap)
  val fullLoader = new TransactionFullLoader(partitions, lastTransactionsMap)

  val consumerPartitions: Set[Int] = consumer.getPartitions

  if (!partitions.subsetOf(consumerPartitions))
    throw new IllegalArgumentException("PE ${id} - Partition set which is used in ProcessingEngine is not subset of Consumer's partitions.")

  partitions
    .foreach(p => {
      setLastPartitionActivity(p)
      lastTransactionsMap(p) = TransactionState(
        transactionID = consumer.getCurrentOffset(p),
        status = TransactionStates.Checkpointed,
        partition = p,
        masterID = -1,
        orderID = -1,
        count = -1,
        ttlMs = -1,
        authKey = authKey)
    })

  /**
    * Reads transaction states from database or fast load and does self-kick if no events.
    *
    * @param pollTimeMs
    */
  def processReadyTransactions(pollTimeMs: Int): Unit = {
    Try {
      if (!isThresholdsSet.get()) {
        isThresholdsSet.set(true)

        loadExecutor.setThresholds(queueLengthThreshold = 1000, taskFullDelayThresholdMs = 150,
          taskDelayThresholdMs = 100, taskRunDelayThresholdMs = 50)
      }

      var loadFullDataExists = false

      val seq = queue.get(pollTimeMs, TimeUnit.MILLISECONDS)

      if (Subscriber.logger.isDebugEnabled())
        Subscriber.logger.debug(s"$seq")

      if (Option(seq).nonEmpty) {
        isFirstTime = false
        if (seq.nonEmpty) {
          if (fastLoader.checkIfTransactionLoadingIsPossible(seq)) {
            fastLoader.load(seq, consumer, loadExecutor, callback)
          }
          else {
            if (fullLoader.checkIfTransactionLoadingIsPossible(seq)) {
              ProcessingEngine.logger.warn(s"PE $id - Load full occurred for seq $seq")
              if (fullLoader.load(seq, consumer, loadExecutor, callback) > 0)
                loadFullDataExists = true
            } else {
              Subscriber.logger.warn(s"Fast and Full loading failed for $seq.")
            }
          }
          setLastPartitionActivity(seq.head.partition)
        }
      }

      enqueueTransactionStateWhenNecessary(loadFullDataExists, pollTimeMs)
    } match {
      case Failure(exception) =>
        callback.onFailureCall(consumer, exception)
        isRunning.set(false)
        throw exception

      case _ =>
    }
  }

  private def enqueueTransactionStateWhenNecessary(loadFullDataExists: Boolean, pollTimeMs: Int) = {
    partitions
      .foreach(p =>
        if ((loadFullDataExists && queue.getInFlight == 0)
          || isFirstTime
          || (System.currentTimeMillis() - getLastPartitionActivity(p) > pollTimeMs && queue.getInFlight == 0)) {
          enqueueLastPossibleTransactionState(p)
        })

    isFirstTime = false
  }

  /**
    * Enqueues in queue last transaction from database
    */
  private[tstreams] def enqueueLastPossibleTransactionState(partition: Int): Unit = {
    assert(partitions.contains(partition))

    val proposedTransactionId = consumer.getProposedTransactionId

    val transactionStates = List(TransactionState(
      transactionID = proposedTransactionId,
      partition = partition,
      masterID = 0,
      orderID = 0,
      count = -1,
      status = TransactionStates.Checkpointed,
      ttlMs = -1,
      authKey = authKey))

    if (Subscriber.logger.isDebugEnabled())
      Subscriber.logger.debug(s"Enqueued $transactionStates")

    queue.put(transactionStates)
  }

  def start(): Unit = {
    if (!isRunning.getAndSet(true))
      executor.start()
  }

  def stop(): Unit = {
    if (isRunning.getAndSet(false)) {
      partitions.foreach(p => enqueueLastPossibleTransactionState(p))
      executor.join(Subscriber.SHUTDOWN_WAIT_MAX_SECONDS * 1000)
    }
  }
}


private[tstreams] object ProcessingEngine {

  // val PROTECTION_INTERVAL = 10

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  type LastTransactionStateMapType = mutable.Map[Int, TransactionState]

  class CallbackTask(consumer: TransactionOperator, transactionState: TransactionState, callback: Callback) extends Runnable {

    override def toString: String = s"CallbackTask($transactionState)"

    override def run(): Unit = {
      callback.onTransactionCall(
        consumer = consumer,
        partition = transactionState.partition,
        transactionID = transactionState.transactionID,
        count = transactionState.count)
    }
  }

}