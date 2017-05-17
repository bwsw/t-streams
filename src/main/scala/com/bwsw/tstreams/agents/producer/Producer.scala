package com.bwsw.tstreams.agents.producer

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.group.{CheckpointGroup, CheckpointInfo, GroupParticipant, SendingAgent}
import com.bwsw.tstreams.agents.producer.NewProducerTransactionPolicy.ProducerPolicy
import com.bwsw.tstreams.common._
import com.bwsw.tstreams.coordination.client.UdpEventsBroadcastClient
import com.bwsw.tstreams.proto.protocol.{TransactionRequest, TransactionResponse, TransactionState}
import com.bwsw.tstreams.storage.StorageClient
import com.bwsw.tstreams.streams.Stream
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
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
  extends GroupParticipant with SendingAgent with Interaction {

  /**
    * agent name
    */
  override private[tstreams] def getAgentName() = name

  // short key
  val pcs = producerOptions.coordinationOptions
  val isStopped = new AtomicBoolean(false)
  val isMissedUpdate = new AtomicBoolean(false)

  private[tstreams] val openTransactions = new OpenTransactionsKeeper()

  private val peerKeepAliveTimeout = pcs.zkSessionTimeoutMs * 2

  val fullPrefix = java.nio.file.Paths.get(pcs.zkPrefix, stream.name).toString.substring(1)
  private val curatorClient = CuratorFrameworkFactory.builder()
    .namespace(fullPrefix)
    .connectionTimeoutMs(pcs.zkConnectionTimeoutMs)
    .sessionTimeoutMs(pcs.zkSessionTimeoutMs)
    .retryPolicy(new ExponentialBackoffRetry(1000, 3))
    .connectString(pcs.zkEndpoints).build()

  curatorClient.start()

  try {
    curatorClient.create().creatingParentContainersIfNeeded().forPath("/subscribers")
  } catch {
    case e: KeeperException =>
      if (e.code() != KeeperException.Code.NODEEXISTS)
        throw e
  }

  // amount of threads which will handle partitions in masters, etc
  val threadPoolSize: Int = {
    if (pcs.threadPoolSize == -1)
      producerOptions.writePolicy.getUsedPartitions.size
    else
      pcs.threadPoolSize
  }


  // this client is used to find new subscribers
  private[tstreams] val subscriberNotifier = new UdpEventsBroadcastClient(curatorClient, partitions = producerOptions.writePolicy.getUsedPartitions)
  subscriberNotifier.init()

  /**
    * Allows to publish update/pre/post/cancel messages.
    *
    * @param msg
    * @return
    */
  def publish(msg: TransactionState) = subscriberNotifier.publish(msg)

  // pcs.transportClientRetryDelayMs
  private[tstreams] val openTransactionClient = new UdpClient(pcs.transportClientTimeoutMs).start()

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
      isInstant = isInstant, data = data.map(com.google.protobuf.ByteString.copyFrom))
    openTransactionClient.sendAndWait(host, port, r)
  }


  /**
    * P2P Agent for producers interaction
    * (getNewTransaction id; publish openTransaction event; publish closeTransaction event)
    */
  override private[tstreams] val transactionOpenerService: TransactionOpenerService = new TransactionOpenerService(
    curatorClient = curatorClient,
    peerKeepAliveTimeout = peerKeepAliveTimeout,
    producer = this,
    usedPartitions = producerOptions.writePolicy.getUsedPartitions,
    threadPoolAmount = threadPoolSize)

  Producer.logger.info(s"Start new Basic producer with id: ${transactionOpenerService.getUniqueAgentID()}, name : $name, streamName : ${stream.name}, streamPartitions : ${stream.partitionsCount}")

  /**
    * Queue to figure out moment when transaction is going to close
    */
  private val shutdownKeepAliveThread = new ThreadSignalSleepVar[Boolean](1)
  private val transactionKeepAliveThread = getTransactionKeepAliveThread
  private[tstreams] val notifyService = new FirstFailLockableTaskExecutor(s"NotifyService-$name", producerOptions.notifyJobsThreadPoolSize)
  private lazy val cg = new CheckpointGroup()

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
    lazy val message = s"Producer $name[${transactionOpenerService.getUniqueAgentID()}] missed transaction ttl interval. " +
      s"Last was $lastUpdateEndTime, now is $currentTime. It's critical situation, it is marked as non functional, only stop is allowed."

    if(isMissedUpdate.get())
      throw new IllegalStateException(message)

    if(currentTime - lastUpdateEndTime > producerOptions.transactionTtlMs) {
      Producer.logger.error(message)
      isMissedUpdate.set(true)
      throw new IllegalStateException(message)
    }
  }

  private[tstreams] def checkStopped(setState: Boolean = false) = {
    if (isStopped.getAndSet(setState))
      throw new IllegalStateException(s"Producer ${this.name}[${transactionOpenerService.getUniqueAgentID()}] is already stopped. Unable to get new transaction.")
  }

  /**
    *
    */
  private [tstreams] var lastUpdateEndTime = System.currentTimeMillis()
  private def getTransactionKeepAliveThread: Thread = {
    val latch = new CountDownLatch(1)
    val transactionKeepAliveThread = new Thread(() => {
      Thread.currentThread().setName(s"Producer-$name[${transactionOpenerService.getUniqueAgentID()}]-KeepAlive")
      latch.countDown()
      Producer.logger.info(s"Producer $name[${transactionOpenerService.getUniqueAgentID()}] - object is started, launched open transaction update thread")
      var isExit = false
      while (!isExit) {
        isExit = shutdownKeepAliveThread.wait(producerOptions.transactionKeepAliveMs)
        if (isExit) {
          Producer.logger.info(s"Producer $name[${transactionOpenerService.getUniqueAgentID()}] - object shutdown is requested. Exit KeepAliveThread.")
        } else {
          // do update
          if(Producer.logger.isDebugEnabled())
            Producer.logger.debug(s"Producer $name[${transactionOpenerService.getUniqueAgentID()}] - update is started for long lasting transactions")
          val transactionStates = openTransactions.forallKeysDo((part: Int, transaction: IProducerTransaction) => transaction.getUpdateInfo)
          stream.client.putTransactions(transactionStates.flatten.toSeq, Seq())
          if(Producer.logger.isDebugEnabled())
            Producer.logger.debug(s"Producer $name[${transactionOpenerService.getUniqueAgentID()}] - update is completed for long lasting transactions")
          // check if update is missed
          val currentUpdateEndTime = System.currentTimeMillis()
          if (currentUpdateEndTime - lastUpdateEndTime > producerOptions.transactionTtlMs) {
            isMissedUpdate.set(true)
            isExit = true
            if(Producer.logger.isDebugEnabled())
              Producer.logger.error(s"Producer $name[${transactionOpenerService.getUniqueAgentID()}] missed transaction ttl interval. " +
                s"Last was $lastUpdateEndTime, now is $currentUpdateEndTime. " +
                "It's critical situation, it is marked as non functional, only stop is allowed.")
          }
          openTransactions.forallKeysDo((part: Int, transaction: IProducerTransaction) => transaction.notifyUpdate())
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
      throw new IllegalArgumentException(s"Producer $name[${transactionOpenerService.getUniqueAgentID()}] - invalid partition $partition")

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
  def newTransaction(policy: ProducerPolicy = NewProducerTransactionPolicy.ErrorIfOpened, partition: Int = -1): ProducerTransaction = {
    checkStopped()
    checkUpdateFailure()

    val evaluatedPartition = {
      if (partition == -1)
        producerOptions.writePolicy.getNextPartition
      else
        partition
    }

    if (Producer.logger.isDebugEnabled)
      Producer.logger.debug(s"Evaluate a partition for new transaction [PARTITION_$evaluatedPartition]")

    if (!producerOptions.writePolicy.getUsedPartitions.contains(evaluatedPartition))
      throw new IllegalArgumentException(s"Producer $name[${transactionOpenerService.getUniqueAgentID()}] - invalid partition $evaluatedPartition")

    if (Producer.logger.isDebugEnabled)
      Producer.logger.debug(s"Producer $name[${transactionOpenerService.getUniqueAgentID()}] [PARTITION_$evaluatedPartition] Handle the previous opened transaction if it exists")
    val previousTransactionAction = openTransactions.handlePreviousOpenTransaction(evaluatedPartition, policy)
    if (previousTransactionAction != null) {
      if (Producer.logger.isDebugEnabled)
        Producer.logger.debug(s"Producer $name[${transactionOpenerService.getUniqueAgentID()}] [PARTITION_$evaluatedPartition] The previous opened transaction exists so do an action")
      previousTransactionAction()
    }

    if (Producer.logger.isDebugEnabled)
      Producer.logger.debug(s"Producer $name[${transactionOpenerService.getUniqueAgentID()}] [PARTITION_$evaluatedPartition] Start generating a new transaction id")
    val transactionID = transactionOpenerService.generateNewTransaction(evaluatedPartition)


    if (Producer.logger.isDebugEnabled)
      Producer.logger.debug(s"Producer $name[${transactionOpenerService.getUniqueAgentID()}] [NEW_TRANSACTION PARTITION_$evaluatedPartition] ID=$transactionID")

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
      throw new IllegalArgumentException(s"Producer $name[${transactionOpenerService.getUniqueAgentID()}] - invalid partition")
    openTransactions.getTransactionSetOption(partition).map(v => v._2.filter(!_.isClosed))
  }

  /**
    * Checkpoint all opened transactions (atomic).
    */
  val firstCheckpoint = new AtomicBoolean(true)

  def checkpoint() = {
    checkStopped()
    checkUpdateFailure()
    if (firstCheckpoint.getAndSet(false)) cg.add(this)
    cg.checkpoint()
  }


  private def cancelPendingTransactions() = this.synchronized {
    val transactionStates = openTransactions.forallKeysDo((part: Int, transaction: IProducerTransaction) => transaction.getCancelInfoAndClose)
    stream.client.putTransactions(transactionStates.flatten.toSeq, Seq())
    openTransactions.forallKeysDo((k: Int, v: IProducerTransaction) => v.notifyCancelEvent())
    openTransactions.clear()
  }

  /**
    * Cancel all opened transactions (not atomic, probably atomic is not a case for a cancel).
    */
  def cancel(): Unit = {
    checkStopped()
    checkUpdateFailure()
    cancelPendingTransactions()
  }


  /**
    * Finalize all opened transactions (not atomic). For atomic use CheckpointGroup.
    */
  override private[tstreams] def finalizeDataSend(): Unit = {
    checkStopped()
    checkUpdateFailure()
    openTransactions.forallKeysDo((k: Int, v: IProducerTransaction) => v.finalizeDataSend())
  }

  /**
    * Info to commit
    */
  override private[tstreams] def getCheckpointInfoAndClear(): List[CheckpointInfo] = {
    checkStopped()
    checkUpdateFailure()
    val checkpointInfo = openTransactions.forallKeysDo((k: Int, v: IProducerTransaction) => v.getCheckpointInfo).toList
    openTransactions.clear()
    checkpointInfo
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
      masterID = transactionOpenerService.getUniqueAgentID(),
      orderID = transactionOpenerService.getAndIncSequentialID(partition),
      count = 0)
    subscriberNotifier.publish(msg)
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
      masterID = transactionOpenerService.getUniqueAgentID(),
      orderID = transactionOpenerService.getAndIncSequentialID(partition),
      count = data.size,
      isNotReliable = !isReliable)

    if (Producer.logger.isDebugEnabled())
      Producer.logger.debug(s"Transaction Update Sent: $msgInstant")

    subscriberNotifier.publish(msgInstant)
  }

  /**
    * Stop this agent
    */
  def stop() = {
    Producer.logger.info(s"Producer $name[${transactionOpenerService.getUniqueAgentID()}] is shutting down.")
    cancel()
    checkStopped(true)
    cg.stop()
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
    curatorClient.close()

    openTransactions.clear()

    stream.client.shutdown()
  }


  override private[tstreams] def getStorageClient(): StorageClient = stream.client
}