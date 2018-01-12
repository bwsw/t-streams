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

package com.bwsw.tstreams.agents.producer

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.group._
import com.bwsw.tstreams.agents.producer.NewProducerTransactionPolicy.ProducerPolicy
import com.bwsw.tstreams.common._
import com.bwsw.tstreams.coordination.client.UdpEventsBroadcastClient
import com.bwsw.tstreams.storage.StorageClient
import com.bwsw.tstreams.streams.Stream
import com.bwsw.tstreamstransactionserver.rpc.TransactionState
import org.apache.zookeeper.KeeperException
import org.slf4j.{Logger, LoggerFactory}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.{Failure, Random, Try}


object Producer {
  val logger: Logger = LoggerFactory.getLogger(classOf[Producer])
  var SHUTDOWN_WAIT_MAX_SECONDS: Int = CommonConstants.SHUTDOWN_WAIT_MAX_SECONDS
}

/**
  * Basic producer class
  *
  * @param name            Producer name
  * @param stream          Stream for transaction sending
  * @param producerOptions This producer options
  */
class Producer(var name: String,
               val stream: Stream,
               val producerOptions: ProducerOptions)
  extends GroupParticipant with SendingAgent with AutoCloseable {

  import Producer.logger

  private val isStopped = new AtomicBoolean(false)
  private val isMissedUpdate = new AtomicBoolean(false)
  private val fullPrefix = stream.path

  /**
    * Queue to figure out moment when transaction is going to close
    */
  private val shutdownKeepAliveThread = new ThreadSignalSleepVar[Boolean](1)
  private lazy val transactionKeepAliveThread = getTransactionKeepAliveThread

  private[tstreams] val openTransactions = new OpenTransactionsKeeper()
  private[tstreams] val notifyService =
    new FirstFailLockableTaskExecutor(s"NotifyService-$name", producerOptions.notifyJobsThreadPoolSize)
  private[tstreams] val curatorClient = stream.client.curatorClient
  private[tstreams] val subscriberNotifier = new UdpEventsBroadcastClient(
    curatorClient, prefix = fullPrefix, partitions = producerOptions.writePolicy.getUsedPartitions)
  private[tstreams] var lastUpdateEndTime = System.currentTimeMillis()
  private[tstreams] val getUniqueAgentID = Random.nextLong()
  private[tstreams] val transactionGenerator = new TransactionGenerator(stream.client)


  logger.info(
    s"Start new Basic producer with id: $getUniqueAgentID, name : $name, " +
      "streamName : ${stream.name}, streamPartitions : ${stream.partitionsCount}")

  Try(curatorClient.create().creatingParentContainersIfNeeded().forPath(s"$fullPrefix/subscribers")) match {
    case Failure(e: KeeperException) if e.code() == KeeperException.Code.NODEEXISTS =>
    case Failure(e) => throw e
    case _ =>
  }

  subscriberNotifier.init()
  transactionKeepAliveThread

  /**
    * agent name
    */
  override private[tstreams] def getAgentName() = name

  /**
    * Allows to publish update/pre/post/cancel messages.
    *
    * @param msg
    * @return
    */
  def publish(msg: TransactionState): Unit = subscriberNotifier.publish(msg)

  private[tstreams] def checkUpdateFailure() = {
    val currentTime = System.currentTimeMillis()
    lazy val message =
      s"Producer $name[$getUniqueAgentID] missed transaction ttl interval. " +
        s"Last was $lastUpdateEndTime, now is $currentTime. It's critical situation, " +
        "it is marked as non functional, only stop is allowed."

    if (isMissedUpdate.get())
      throw new MissedUpdateException(message)

    if (currentTime - lastUpdateEndTime > producerOptions.transactionTtlMs) {
      Producer.logger.error(message)
      isMissedUpdate.set(true)
      throw new MissedUpdateException(message)
    }
    val avail = producerOptions.transactionTtlMs - (currentTime - lastUpdateEndTime) - 1

    avail.milliseconds
  }

  private[tstreams] def checkStopped(setState: Boolean = false) = {
    if (isStopped.getAndSet(setState))
      throw new IllegalStateException(
        s"Producer $name[$getUniqueAgentID] is already stopped. Unable to get new transaction.")
  }

  private def getTransactionKeepAliveThread: Thread = {
    val latch = new CountDownLatch(1)
    val transactionKeepAliveThread = new Thread(() => {
      Thread.currentThread().setName(s"Producer-$name[$getUniqueAgentID]-KeepAlive")
      latch.countDown()
      logger.info(s"Producer $name[$getUniqueAgentID] - object is started, launched open transaction update thread")
      var isExit = false

      @tailrec
      def loop(): Unit = {
        if (isExit || shutdownKeepAliveThread.wait(producerOptions.transactionKeepAliveMs)) {
          logger.info(s"Producer $name[$getUniqueAgentID] - object shutdown is requested. Exit KeepAliveThread.")
        } else {
          // do update
          if (logger.isDebugEnabled())
            logger.debug(s"Producer $name[$getUniqueAgentID] - update is started for long lasting transactions")

          val transactionStates = openTransactions.forallTransactionsDo((_, transaction) => transaction.getUpdateInfo())
          stream.client.putTransactions(transactionStates.flatten.toSeq, Seq())

          if (logger.isDebugEnabled())
            logger.debug(s"Producer $name[$getUniqueAgentID] - update is completed for long lasting transactions")

          // check if update is missed
          val currentUpdateEndTime = System.currentTimeMillis()
          if (currentUpdateEndTime - lastUpdateEndTime > producerOptions.transactionTtlMs) {
            isMissedUpdate.set(true)
            isExit = true
            if (logger.isDebugEnabled())
              logger.debug(s"Producer $name[$getUniqueAgentID] missed transaction ttl interval. " +
                s"Last was $lastUpdateEndTime, now is $currentUpdateEndTime. " +
                "It's critical situation, it is marked as non functional, only stop is allowed.")
          }
          openTransactions.forallTransactionsDo((_, transaction) => transaction.notifyUpdate())
          lastUpdateEndTime = currentUpdateEndTime

          loop()
        }
      }

      loop()
    })

    transactionKeepAliveThread.start()
    latch.await()

    transactionKeepAliveThread
  }

  def generateNewTransaction(partition: Int,
                             isInstant: Boolean = false,
                             isReliable: Boolean = true,
                             data: Seq[Array[Byte]] = Seq()): Long = {
    (isInstant, isReliable) match {
      case (true, true) =>
        getStorageClient().putInstantTransactionSync(stream.id, partition, data)

      case (true, false) =>
        getStorageClient().putInstantTransactionUnreliable(stream.id, partition, data)

        0

      case (false, _) =>
        getStorageClient().openTransactionSync(stream.id, partition, producerOptions.transactionTtlMs)
    }
  }

  /**
    * Instant transaction send out (kafka-like)
    * The method implements "at-least-once", which means that some packets might be sent more than once.
    * If you want your data in storage for sure, then don't use with isReliable == false.
    * Overall package size must not exceed UDP maximum payload size, which is specified in [[com.bwsw.tstreams.common.UdpProcessor]]
    * in BUFFER_SIZE, which is 508 bytes by default but may be increased if your network supports it (ideally may be used with jumbo
    * frames enabled).
    * The method is blocking.
    *
    * @param partition  partition to write transaction and data
    * @param data
    * @param isReliable either master waits for storage server reply or not
    *                   (if is not reliable then it leads to at-least-once with possible losses)
    * @return transaction ID
    */
  def instantTransaction(partition: Int, data: Seq[Array[Byte]], isReliable: Boolean): Long = {
    checkStopped()
    checkUpdateFailure()
    if (!producerOptions.writePolicy.getUsedPartitions.contains(partition))
      throw new IllegalArgumentException(s"Producer $name[$getUniqueAgentID] - invalid partition $partition")

    generateNewTransaction(partition = partition, isInstant = true, isReliable = isReliable, data = data)
  }

  /**
    * Wrapper method when the partition is automatically selected with writePolicy (round robin).
    * The method is blocking.
    *
    * @param data
    * @param isReliable
    * @return
    */
  def instantTransaction(data: Seq[Array[Byte]], isReliable: Boolean): Long =
    instantTransaction(producerOptions.writePolicy.getNextPartition, data, isReliable)

  /**
    * Regular long-living transaction creation. The method allows doing reliable long-living transactions with
    * exactly-once semantics.
    * The method is blocking.
    *
    * @param policy    the policy to use when open new transaction for the partition which already has opened transaction.
    *                  See [[NewProducerTransactionPolicy]] for details.
    * @param partition if -1 specified (default) then the method uses writePolicy (round robin)
    * @return new transaction object
    */
  def newTransaction(policy: ProducerPolicy = NewProducerTransactionPolicy.EnqueueIfOpened,
                     partition: Int = -1): ProducerTransactionImpl = {
    checkStopped()
    checkUpdateFailure()

    val evaluatedPartition = {
      if (partition == -1)
        producerOptions.writePolicy.getNextPartition
      else
        partition
    }

    if (logger.isDebugEnabled) logger.debug(s"Evaluate a partition for new transaction [PARTITION_$evaluatedPartition]")

    if (!producerOptions.writePolicy.getUsedPartitions.contains(evaluatedPartition))
      throw new IllegalArgumentException(s"Producer $name[$getUniqueAgentID] - invalid partition $evaluatedPartition")

    if (logger.isDebugEnabled)
      logger.debug(
        s"Producer $name[$getUniqueAgentID] [PARTITION_$evaluatedPartition] " +
          "Handle the previous opened transaction if it exists")

    val previousTransactionAction = openTransactions.handlePreviousOpenTransaction(evaluatedPartition, policy)

    if (logger.isDebugEnabled)
      logger.debug(
        s"Producer $name[$getUniqueAgentID] [PARTITION_$evaluatedPartition] " +
          "The previous opened transaction exists so do an action")

    previousTransactionAction()

    if (logger.isDebugEnabled)
      logger.debug(
        s"Producer $name[$getUniqueAgentID] [PARTITION_$evaluatedPartition] Start generating a new transaction id")

    val transactionID = generateNewTransaction(evaluatedPartition)

    if (logger.isDebugEnabled)
      logger.debug(
        s"Producer $name[$getUniqueAgentID] [NEW_TRANSACTION PARTITION_$evaluatedPartition] ID=$transactionID")

    val transaction = new ProducerTransactionImpl(evaluatedPartition, transactionID, this)
    openTransactions.put(evaluatedPartition, transaction)

    transaction
  }


  /**
    * Return transaction for specific partition if there is opened one.
    *
    * @param partition Partition from which transaction will be retrieved
    * @return Transaction reference if it exist and is opened
    */
  private[tstreams] def getOpenedTransactionsForPartition(partition: Int): Option[mutable.Set[ProducerTransaction]] = {
    if (partition < 0 || partition >= stream.partitionsCount)
      throw new IllegalArgumentException(s"Producer $name[$getUniqueAgentID] - invalid partition")

    openTransactions.getTransactionSetOption(partition).map(v => v._2.filter(!_.isClosed))
  }

  private def doCheckpoint(checkpointRequests: Seq[State]) = {
    val producerRequests = checkpointRequests.map {
      case ProducerTransactionState(_, _, _, rpcTransaction) => rpcTransaction
      case _ => throw new IllegalStateException("Only ProducerCheckpointInfo allowed here.")
    }
    val availTime = checkUpdateFailure()

    stream.client.putTransactions(producerRequests, Seq(), availTime)
  }

  private def notifyCheckpoint(checkpointInfo: Seq[State]) = {
    checkpointInfo foreach {
      case ProducerTransactionState(_, agent, checkpointEvent, _) =>
        notifyService.submit(
          "<CheckpointEvent>",
          () => agent.publish(checkpointEvent),
          None)
      case _ =>
    }
  }

  def checkpoint(): Producer = this.synchronized {
    checkStopped()
    checkUpdateFailure()
    finalizeDataSend()
    val checkpointRequests = getStateAndClear()
    doCheckpoint(checkpointRequests)
    notifyCheckpoint(checkpointRequests)

    this
  }

  def checkpoint(partition: Int): Producer = this.synchronized {
    checkStopped()
    checkUpdateFailure()
    finalizePartitionDataSend(partition)
    val checkpointRequests = getPartitionCheckpointInfoAndClear(partition)
    doCheckpoint(checkpointRequests)
    notifyCheckpoint(checkpointRequests)

    this
  }

  private def cancelPendingTransactions() = this.synchronized {
    val transactionStates =
      openTransactions.forallTransactionsDo((_, transaction) => transaction.getCancelInfoAndClose())
    stream.client.putTransactions(transactionStates.flatten.toSeq, Seq())
    openTransactions.forallTransactionsDo((_, v) => v.notifyCancelEvent())
    openTransactions.clear()
  }

  /**
    * Cancel all opened transactions (not atomic, probably atomic is not a case for a cancel).
    */
  def cancel(): Producer = {
    checkStopped()
    checkUpdateFailure()
    cancelPendingTransactions()

    this
  }

  def cancel(partition: Int): Option[(Long, mutable.Set[ProducerTransaction])] = {
    openTransactions.getTransactionSetOption(partition).foreach {
      case (_, transactions) =>
        val transactionStates = transactions.flatMap(_.getCancelInfoAndClose()).toSeq
        stream.client.putTransactions(transactionStates, Seq())
    }
    openTransactions.forPartitionTransactionsDo(partition, _.notifyCancelEvent())

    openTransactions.clear(partition)
  }

  /**
    * Finalize all opened transactions (not atomic). For atomic use CheckpointGroup.
    */
  override private[tstreams] def finalizeDataSend(): Unit = this.synchronized {
    checkStopped()
    checkUpdateFailure()
    openTransactions.forallTransactionsDo((_, t) => t.finalizeDataSend())
  }

  private def finalizePartitionDataSend(partition: Int): Unit = this.synchronized {
    checkStopped()
    checkUpdateFailure()
    openTransactions.forPartitionTransactionsDo(partition, _.finalizeDataSend())
  }

  /**
    * Info to commit
    */
  override private[tstreams] def getStateAndClear(): Array[State] = getInfoAndClear(true)

  private[tstreams] def getCancelInfoAndClear(): Array[State] = getInfoAndClear(false)

  private def getInfoAndClear(checkpoint: Boolean): Array[State] = this.synchronized {
    checkStopped()
    checkUpdateFailure()
    val stateInfo = openTransactions.forallTransactionsDo((_, v) => v.getStateInfo(checkpoint))
    openTransactions.clear()

    stateInfo.toArray
  }

  private def getPartitionCheckpointInfoAndClear(partition: Int): Array[State] = this.synchronized {
    checkStopped()
    checkUpdateFailure()
    val res = openTransactions.getTransactionSetOption(partition)
      .map(partData => partData._2.map(txn => txn.getStateInfo(true)))
    openTransactions.clear(partition)

    res.map(_.toArray[State]).getOrElse(Array.empty)
  }

  /**
    * Stop this agent
    */
  def stop(): Unit = this.synchronized {
    try {
      logger.info(s"Producer $name[$getUniqueAgentID] is shutting down.")
      cancel()
      checkStopped(true)

      // stop update state of all open transactions
      shutdownKeepAliveThread.signal(true)
      transactionKeepAliveThread.join()

      // stop executors
      notifyService.shutdownOrDie(Producer.SHUTDOWN_WAIT_MAX_SECONDS, TimeUnit.SECONDS)

      // stop function which works with subscribers
      subscriberNotifier.stop()

      openTransactions.clear()
    } finally {
      stream.shutdown()
    }
  }

  override private[tstreams] def getStorageClient(): StorageClient = stream.client

  override def close(): Unit = if (!isStopped.get()) stop()
}