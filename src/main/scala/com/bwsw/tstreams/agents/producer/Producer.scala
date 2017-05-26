package com.bwsw.tstreams.agents.producer

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.group._
import com.bwsw.tstreams.agents.producer.NewProducerTransactionPolicy.ProducerPolicy
import com.bwsw.tstreams.common._
import com.bwsw.tstreams.coordination.client.UdpEventsBroadcastClient
import com.bwsw.tstreams.proto.protocol.{TransactionRequest, TransactionResponse, TransactionState}
import com.bwsw.tstreams.storage.StorageClient
import com.bwsw.tstreams.streams.Stream
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import org.apache.zookeeper.KeeperException
import org.slf4j.LoggerFactory

import scala.concurrent.duration._


object Producer {
  val logger = LoggerFactory.getLogger(this.getClass)
  var SHUTDOWN_WAIT_MAX_SECONDS = GeneralOptions.SHUTDOWN_WAIT_MAX_SECONDS
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
  extends GroupParticipant with SendingAgent with Interaction with AutoCloseable {

  // short key
  val pcs = producerOptions.coordinationOptions
  val isStopped = new AtomicBoolean(false)
  val isMissedUpdate = new AtomicBoolean(false)
  val fullPrefix = java.nio.file.Paths.get(pcs.zkPrefix, stream.name).toString

  /**
    * Queue to figure out moment when transaction is going to close
    */
  private val shutdownKeepAliveThread = new ThreadSignalSleepVar[Boolean](1)
  private lazy val transactionKeepAliveThread = getTransactionKeepAliveThread
  private val threadPoolSize = calculateThreadPoolSize
  private val log = Producer.logger

  private[tstreams] val openTransactions = new OpenTransactionsKeeper()
  private[tstreams] val notifyService = new FirstFailLockableTaskExecutor(s"NotifyService-$name", producerOptions.notifyJobsThreadPoolSize)
  private[tstreams] val curatorClient = stream.client.curatorClient
  private[tstreams] val subscriberNotifier = new UdpEventsBroadcastClient(curatorClient, prefix = fullPrefix, partitions = producerOptions.writePolicy.getUsedPartitions)
  private[tstreams] var lastUpdateEndTime = System.currentTimeMillis()
  private[tstreams] val openTransactionClient = new UdpClient(pcs.transportClientTimeoutMs).start()

  private[tstreams] val transactionOpenerService = new TransactionOpenerService(
    curatorClient = curatorClient,
    prefix = fullPrefix,
    producer = this,
    usedPartitions = producerOptions.writePolicy.getUsedPartitions,
    threadPoolAmount = threadPoolSize)

  log.info(s"Start new Basic producer with id: ${transactionOpenerService.getUniqueAgentID}, name : $name, streamName : ${stream.name}, streamPartitions : ${stream.partitionsCount}")

  try {
    curatorClient.create().creatingParentContainersIfNeeded().forPath(s"$fullPrefix/subscribers")
  } catch {
    case e: KeeperException =>
      if (e.code() != KeeperException.Code.NODEEXISTS)
        throw e
  }

  subscriberNotifier.init()
  transactionKeepAliveThread

  private def calculateThreadPoolSize: Int = {
    if (pcs.threadPoolSize == -1)
      producerOptions.writePolicy.getUsedPartitions.size
    else
      pcs.threadPoolSize
  }

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
  def publish(msg: TransactionState, authKey: String) = subscriberNotifier.publish(msg.withAuthKey(authKey))

  /**
    * Request to get Transaction
    *
    * @param to
    * @param partition
    * @return TransactionResponse or null
    */
  private[tstreams] def transactionRequest(to: String, partition: Int, isInstant: Boolean, isReliable: Boolean, data: Seq[Array[Byte]]): Option[TransactionResponse] = {
    val splits = to.split(":")
    val (host, port) = (splits(0), splits(1).toInt)
    val r = TransactionRequest(partition = partition, isReliable = isReliable,
      isInstant = isInstant, data = data.map(com.google.protobuf.ByteString.copyFrom),
      authKey = stream.client.authenticationKey)
    openTransactionClient.sendAndWait(host, port, r)
  }

  /**
    * Allows to get if the producer is master for the partition.
    *
    * @param partition
    * @return
    */
  private[tstreams] def isMasterOfPartition(partition: Int): Boolean =
    transactionOpenerService.isMasterOfPartition(partition)

  private[tstreams] def getPartitionMasterIDLocalInfo(partition: Int): Int =
    transactionOpenerService.getPartitionMasterInetAddressLocal(partition)._2

  private[tstreams] def checkUpdateFailure() = {
    val currentTime = System.currentTimeMillis()
    lazy val message = s"Producer $name[${transactionOpenerService.getUniqueAgentID}] missed transaction ttl interval. " +
      s"Last was $lastUpdateEndTime, now is $currentTime. It's critical situation, it is marked as non functional, only stop is allowed."

    if(isMissedUpdate.get())
      throw new MissedUpdateException(message)

    if(currentTime - lastUpdateEndTime > producerOptions.transactionTtlMs) {
      Producer.logger.error(message)
      isMissedUpdate.set(true)
      throw new MissedUpdateException(message)
    }
    val avail = producerOptions.transactionTtlMs - (currentTime - lastUpdateEndTime) - 1
    avail.milliseconds
  }

  private[tstreams] def checkStopped(setState: Boolean = false) = {
    if (isStopped.getAndSet(setState))
      throw new IllegalStateException(s"Producer ${this.name}[${transactionOpenerService.getUniqueAgentID}] is already stopped. Unable to get new transaction.")
  }

  /**
    *
    */
  private def getTransactionKeepAliveThread: Thread = {
    val latch = new CountDownLatch(1)
    val transactionKeepAliveThread = new Thread(() => {
      Thread.currentThread().setName(s"Producer-$name[${transactionOpenerService.getUniqueAgentID}]-KeepAlive")
      latch.countDown()
      log.info(s"Producer $name[${transactionOpenerService.getUniqueAgentID}] - object is started, launched open transaction update thread")
      var isExit = false
      while (!isExit) {
        isExit = shutdownKeepAliveThread.wait(producerOptions.transactionKeepAliveMs)
        if (isExit) {
          log.info(s"Producer $name[${transactionOpenerService.getUniqueAgentID}] - object shutdown is requested. Exit KeepAliveThread.")
        } else {
          // do update
          if(log.isDebugEnabled())
            log.debug(s"Producer $name[${transactionOpenerService.getUniqueAgentID}] - update is started for long lasting transactions")

          val transactionStates = openTransactions.forallTransactionsDo((part: Int, transaction: IProducerTransaction) => transaction.getUpdateInfo)
          stream.client.putTransactions(transactionStates.flatten.toSeq, Seq())

          if(log.isDebugEnabled())
            log.debug(s"Producer $name[${transactionOpenerService.getUniqueAgentID}] - update is completed for long lasting transactions")

          // check if update is missed
          val currentUpdateEndTime = System.currentTimeMillis()
          if (currentUpdateEndTime - lastUpdateEndTime > producerOptions.transactionTtlMs) {
            isMissedUpdate.set(true)
            isExit = true
            if(log.isDebugEnabled())
              log.debug(s"Producer $name[${transactionOpenerService.getUniqueAgentID}] missed transaction ttl interval. " +
                s"Last was $lastUpdateEndTime, now is $currentUpdateEndTime. " +
                "It's critical situation, it is marked as non functional, only stop is allowed.")
          }
          openTransactions.forallTransactionsDo((part: Int, transaction: IProducerTransaction) => transaction.notifyUpdate())
          lastUpdateEndTime = currentUpdateEndTime
        }
      }

    })
    transactionKeepAliveThread.start()
    latch.await()
    transactionKeepAliveThread
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
      throw new IllegalArgumentException(s"Producer $name[${transactionOpenerService.getUniqueAgentID}] - invalid partition $partition")

    transactionOpenerService.generateNewTransaction(partition = partition,
      isInstant = true, isReliable = isReliable, data = data)
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
  def newTransaction(policy: ProducerPolicy = NewProducerTransactionPolicy.EnqueueIfOpened, partition: Int = -1): ProducerTransaction = {
    checkStopped()
    checkUpdateFailure()

    val evaluatedPartition = {
      if (partition == -1)
        producerOptions.writePolicy.getNextPartition
      else
        partition
    }

    if (log.isDebugEnabled) log.debug(s"Evaluate a partition for new transaction [PARTITION_$evaluatedPartition]")

    if (!producerOptions.writePolicy.getUsedPartitions.contains(evaluatedPartition))
      throw new IllegalArgumentException(s"Producer $name[${transactionOpenerService.getUniqueAgentID}] - invalid partition $evaluatedPartition")

    if (log.isDebugEnabled)
      log.debug(s"Producer $name[${transactionOpenerService.getUniqueAgentID}] [PARTITION_$evaluatedPartition] Handle the previous opened transaction if it exists")

    val previousTransactionAction = openTransactions.handlePreviousOpenTransaction(evaluatedPartition, policy)
    if (previousTransactionAction != null) {

      if (log.isDebugEnabled)
        log.debug(s"Producer $name[${transactionOpenerService.getUniqueAgentID}] [PARTITION_$evaluatedPartition] The previous opened transaction exists so do an action")

      previousTransactionAction()
    }

    if (log.isDebugEnabled)
      log.debug(s"Producer $name[${transactionOpenerService.getUniqueAgentID}] [PARTITION_$evaluatedPartition] Start generating a new transaction id")

    val transactionID = transactionOpenerService.generateNewTransaction(evaluatedPartition)


    if (log.isDebugEnabled)
      log.debug(s"Producer $name[${transactionOpenerService.getUniqueAgentID}] [NEW_TRANSACTION PARTITION_$evaluatedPartition] ID=$transactionID")

    val transaction = new ProducerTransaction(evaluatedPartition, transactionID, this)
    openTransactions.put(evaluatedPartition, transaction)

    transaction
  }


  /**
    * Return transaction for specific partition if there is opened one.
    *
    * @param partition Partition from which transaction will be retrieved
    * @return Transaction reference if it exist and is opened
    */
  private[tstreams] def getOpenedTransactionsForPartition(partition: Int): Option[scala.collection.mutable.Set[IProducerTransaction]] = {
    if (!(partition >= 0 && partition < stream.partitionsCount))
      throw new IllegalArgumentException(s"Producer $name[${transactionOpenerService.getUniqueAgentID}] - invalid partition")
    openTransactions.getTransactionSetOption(partition).map(v => v._2.filter(!_.isClosed))
  }

  private def doCheckpoint(checkpointRequests: Seq[StateInfo]) = {
    val producerRequests = checkpointRequests.map {
      case ProducerTransactionStateInfo(_, _, _, streamName, partition, transaction, totalCnt, ttl) =>
        new RPCProducerTransaction(streamName, partition, transaction, TransactionStates.Checkpointed, totalCnt, ttl)
      case _ => throw new IllegalStateException("Only ProducerCheckpointInfo allowed here.")
    }
    val availTime = checkUpdateFailure()
    stream.client.putTransactions(producerRequests, Seq(), availTime)
  }

  private def notifyCheckpoint(checkpointInfo: Seq[StateInfo]) = {
    checkpointInfo foreach {
      case ProducerTransactionStateInfo(_, agent, checkpointEvent, _, _, _, _, _) =>
        notifyService.submit("<CheckpointEvent>", () => agent.getProducer.publish(checkpointEvent, agent.getProducer.stream.client.authenticationKey), None)
      case _ =>
    }
  }

  def checkpoint(): Producer = this.synchronized {
    checkStopped()
    checkUpdateFailure()
    finalizeDataSend()
    val checkpointRequests = getCheckpointInfoAndClear()
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
    val transactionStates = openTransactions.forallTransactionsDo((part: Int, transaction: IProducerTransaction) => transaction.getCancelInfoAndClose)
    stream.client.putTransactions(transactionStates.flatten.toSeq, Seq())
    openTransactions.forallTransactionsDo((k: Int, v: IProducerTransaction) => v.notifyCancelEvent())
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

  def cancel(partition: Int) = {
    val transactionStatesOpt = openTransactions.getTransactionSetOption(partition).map(partData => partData._2.map(txn => txn.getCancelInfoAndClose))
    transactionStatesOpt.map(transactionStates => stream.client.putTransactions(transactionStates.flatten.toSeq, Seq()))
    openTransactions.forPartitionTransactionsDo(partition, _.notifyCancelEvent())
    openTransactions.clear(partition)
  }

  /**
    * Finalize all opened transactions (not atomic). For atomic use CheckpointGroup.
    */
  override private[tstreams] def finalizeDataSend(): Unit = this.synchronized {
    checkStopped()
    checkUpdateFailure()
    openTransactions.forallTransactionsDo((k: Int, t: IProducerTransaction) => t.finalizeDataSend())
  }

  private def finalizePartitionDataSend(partition: Int): Unit = this.synchronized {
    checkStopped()
    checkUpdateFailure()
    openTransactions.forPartitionTransactionsDo(partition, (t: IProducerTransaction) => t.finalizeDataSend())
  }

  /**
    * Info to commit
    */
  override private[tstreams] def getCheckpointInfoAndClear(): List[StateInfo] =  {
    getInfoAndClear(TransactionState.Status.Checkpointed)
  }

  private[tstreams] def getCancelInfoAndClear(): List[StateInfo] = {
    getInfoAndClear(TransactionState.Status.Cancelled)
  }

  private def getInfoAndClear(status: TransactionState.Status) = this.synchronized {
    checkStopped()
    checkUpdateFailure()
    val stateInfo = openTransactions.forallTransactionsDo((k: Int, v: IProducerTransaction) => v.getStateInfo(status)).toList
    openTransactions.clear()
    stateInfo
  }

  private def getPartitionCheckpointInfoAndClear(partition: Int): Seq[StateInfo] = this.synchronized {
    checkStopped()
    checkUpdateFailure()
    val res = openTransactions.getTransactionSetOption(partition).map(partData => partData._2.map(txn => txn.getStateInfo(TransactionState.Status.Checkpointed)))
    openTransactions.clear(partition)
    res.fold(Seq[StateInfo]())(resInt => resInt.toSeq)
  }

  /**
    *
    * @return
    */
  private[tstreams] def generateNewTransactionIDLocal() = producerOptions.transactionGenerator.getTransaction()

  /**
    * Method to implement for concrete producer PeerAgent method
    * Need only if this producer is master
    *
    * @return ID
    */
  override private[tstreams] def openTransactionLocal(transactionID: Long, partition: Int): Unit = {

    val transactionRecord = new RPCProducerTransaction(stream.id, partition, transactionID, TransactionStates.Opened, -1, producerOptions.transactionTtlMs)
    val extTransportTimeOutMs = producerOptions.coordinationOptions.transportClientTimeoutMs

    stream.client.putTransactionSync(transactionRecord, extTransportTimeOutMs.milliseconds)

    val msg = TransactionState(
      transactionID = transactionID,
      ttlMs = producerOptions.transactionTtlMs,
      status = TransactionState.Status.Opened,
      partition = partition,
      masterID = transactionOpenerService.getUniqueAgentID,
      orderID = transactionOpenerService.getAndIncSequentialID(partition),
      count = 0)
    publish(msg, stream.client.authenticationKey)
  }

  private[tstreams] def openInstantTransactionLocal(partition: Int, transactionID: Long, data: Seq[Array[Byte]], isReliable: Boolean) = {
    if (isReliable)
      stream.client.putInstantTransactionSync(stream.id, partition, transactionID, data)
    else
      stream.client.putInstantTransactionUnreliable(stream.id, partition, transactionID, data)

    val msgInstant = TransactionState(
      transactionID = transactionID,
      ttlMs = Long.MaxValue,
      status = TransactionState.Status.Instant,
      partition = partition,
      masterID = transactionOpenerService.getUniqueAgentID,
      orderID = transactionOpenerService.getAndIncSequentialID(partition),
      count = data.size,
      isNotReliable = !isReliable)

    if (log.isDebugEnabled())
      log.debug(s"Transaction Update Sent: $msgInstant")

    publish(msgInstant, stream.client.authenticationKey)
  }

  /**
    * Stop this agent
    */
  def stop() = this.synchronized {
    try {
      log.info(s"Producer $name[${transactionOpenerService.getUniqueAgentID}] is shutting down.")
      cancel()
      checkStopped(true)
      openTransactionClient.stop()
      // stop provide master features to public
      transactionOpenerService.stop()

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

  override def close(): Unit = if(!isStopped.get()) stop()
}