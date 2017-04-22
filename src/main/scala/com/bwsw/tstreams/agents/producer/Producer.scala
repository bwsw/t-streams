package com.bwsw.tstreams.agents.producer

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.group.{CheckpointInfo, GroupParticipant, SendingAgent}
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy.ProducerPolicy
import com.bwsw.tstreams.common._
import com.bwsw.tstreams.coordination.client.UdpEventsBroadcastClient
import com.bwsw.tstreams.proto.protocol.{TransactionRequest, TransactionResponse, TransactionState}
import com.bwsw.tstreams.streams.Stream
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import com.google.protobuf.ByteString
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.KeeperException
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.util.control.Breaks._


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

  def setAgentName(name: String) = {
    this.name = name
  }

  // short key
  val pcs = producerOptions.coordinationOptions
  val isStop = new AtomicBoolean(false)

  private val openTransactions = new OpenTransactionsKeeper()

  // stores latches for materialization await (protects from materialization before main transaction response)
  private val threadLock = new ReentrantLock(true)

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
      producerOptions.writePolicy.getUsedPartitions().size
    else
      pcs.threadPoolSize
  }

  Producer.logger.info(s"Start new Basic producer with name : $name, streamName : ${stream.name}, streamPartitions : ${stream.partitionsCount}")

  // this client is used to find new subscribers
  private[tstreams] val subscriberNotifier = new UdpEventsBroadcastClient(curatorClient, partitions = producerOptions.writePolicy.getUsedPartitions())
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
  def transactionRequest(to: String, partition: Int, isInstant: Boolean, isReliable: Boolean, data: Seq[Array[Byte]]): Option[TransactionResponse] = {
    val splits = to.split(":")
    val (host, port) = (splits(0), splits(1).toInt)
    val r = TransactionRequest(partition = partition, isReliable = isReliable,
      isInstant = isInstant, data = data.map(com.google.protobuf.ByteString.copyFrom(_)))
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
    usedPartitions = producerOptions.writePolicy.getUsedPartitions(),
    threadPoolAmount = threadPoolSize)


  /**
    * Queue to figure out moment when transaction is going to close
    */
  private val shutdownKeepAliveThread = new ThreadSignalSleepVar[Boolean](1)
  private val transactionKeepAliveThread = getTransactionKeepAliveThread
  val asyncActivityService = new FirstFailLockableTaskExecutor(s"Producer $name-AsyncWorker")
  val notifyService = new FirstFailLockableTaskExecutor(s"NotifyService-$name", pcs.notifyThreadPoolSize)


  /**
    * Allows to get if the producer is master for the partition.
    *
    * @param partition
    * @return
    */
  def isMasterOfPartition(partition: Int): Boolean =
    transactionOpenerService.isMasterOfPartition(partition)

  def getPartitionMasterIDLocalInfo(partition: Int): Int =
    transactionOpenerService.getPartitionMasterInetAddressLocal(partition)._2

  /**
    * Utility method which allows waiting while the producer completes partition redistribution process.
    * Used mainly in integration tests.
    */

  /**
    *
    */
  def getTransactionKeepAliveThread: Thread = {
    val latch = new CountDownLatch(1)
    val transactionKeepAliveThread = new Thread(() => {
      Thread.currentThread().setName(s"Producer-$name-KeepAlive")
      latch.countDown()
      Producer.logger.info(s"Producer $name - object is started, launched open transaction update thread")
      breakable {
        while (true) {
          val value: Boolean = shutdownKeepAliveThread.wait(producerOptions.transactionKeepAliveMs)
          if (value) {
            Producer.logger.info(s"Producer $name - object either checkpointed or cancelled. Exit KeepAliveThread.")
            break()
          }
          asyncActivityService.submit("<UpdateOpenedTransactionsTask>", () => updateOpenedTransactions(), Option(threadLock))
        }
      }
    })
    transactionKeepAliveThread.start()
    latch.await()
    transactionKeepAliveThread
  }

  /**
    * Used to send update event to all opened transactions
    */
  private def updateOpenedTransactions() = this.synchronized {
    Producer.logger.debug(s"Producer $name - scheduled for long lasting transactions")
    openTransactions.forallKeysDo((part: Int, transaction: IProducerTransaction) => transaction.updateTransactionKeepAliveState())
  }

  /**
    * instant transaction send out (kafka-like)
    *
    * @param partition
    * @param data
    * @param isReliable
    * @return
    */
  def instantTransaction(partition: Int, data: Seq[Array[Byte]], isReliable: Boolean = true): Long = {
    if (!producerOptions.writePolicy.getUsedPartitions().contains(partition))
      throw new IllegalArgumentException(s"Producer $name - invalid partition ${partition}")

    transactionOpenerService.generateNewTransaction(partition = partition,
      isInstant = true, isReliable = isReliable, data = data)
  }

  /**
    * regular long-living transaction creation
    *
    * @param policy
    * @param partition
    * @return
    */
  def newTransaction(policy: ProducerPolicy = NewTransactionProducerPolicy.ErrorIfOpened, partition: Int = -1): ProducerTransaction = {
    if (isStop.get())
      throw new IllegalStateException(s"Producer ${this.name} is already stopped. Unable to get new transaction.")

    val evaluatedPartition = {
      if (partition == -1)
        producerOptions.writePolicy.getNextPartition
      else
        partition
    }

    if (!producerOptions.writePolicy.getUsedPartitions().contains(evaluatedPartition))
      throw new IllegalArgumentException(s"Producer $name - invalid partition ${evaluatedPartition}")

    val previousTransactionAction = openTransactions.handlePreviousOpenTransaction(evaluatedPartition, policy)
    if (previousTransactionAction != null)
      previousTransactionAction()

    val transactionID = transactionOpenerService.generateNewTransaction(evaluatedPartition)

    if (Producer.logger.isDebugEnabled)
      Producer.logger.debug(s"[NEW_TRANSACTION PARTITION_$evaluatedPartition] ID=$transactionID")

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
  def getOpenedTransactionForPartition(partition: Int): Option[IProducerTransaction] = {
    if (!(partition >= 0 && partition < stream.partitionsCount))
      throw new IllegalArgumentException(s"Producer $name - invalid partition")
    openTransactions.getTransactionOption(partition)
  }

  /**
    * Checkpoint all opened transactions (not atomic). For atomic use CheckpointGroup.
    */
  def checkpoint(isSynchronous: Boolean = true): Unit =
    openTransactions.forallKeysDo((k: Int, v: IProducerTransaction) => v.checkpoint(isSynchronous))


  /**
    * Cancel all opened transactions (not atomic). For atomic use CheckpointGroup.
    */
  def cancel(): Unit =
    openTransactions.forallKeysDo((k: Int, v: IProducerTransaction) => v.cancel())


  /**
    * Finalize all opened transactions (not atomic). For atomic use CheckpointGroup.
    */
  def finalizeDataSend(): Unit = {
    openTransactions.forallKeysDo((k: Int, v: IProducerTransaction) => v.finalizeDataSend())
  }

  /**
    * Info to commit
    */
  override private[tstreams] def getCheckpointInfoAndClear(): List[CheckpointInfo] = {
    val checkpointInfo = openTransactions.forallKeysDo((k: Int, v: IProducerTransaction) => v.getTransactionInfo()).toList
    openTransactions.clear()
    checkpointInfo
  }

  /**
    *
    * @return
    */
  def generateNewTransactionIDLocal() =
    producerOptions.transactionGenerator.getTransaction()

  /**
    * Method to implement for concrete producer PeerAgent method
    * Need only if this producer is master
    *
    * @return ID
    */
  override private[tstreams] def openTransactionLocal(transactionID: Long, partition: Int): Unit = {

    val transactionRecord = new RPCProducerTransaction(stream.name, partition, transactionID, TransactionStates.Opened, -1, producerOptions.transactionTtlMs)
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
    if(isReliable)
      stream.client.putInstantTransactionSync(stream.name, partition, transactionID, data)
    else
      stream.client.putInstantTransactionUnreliable(stream.name, partition, transactionID, data)

    val msgOpened = TransactionState(
      transactionID = transactionID,
      ttlMs = producerOptions.transactionTtlMs,
      status = TransactionState.Status.Opened,
      partition = partition,
      masterID = transactionOpenerService.getUniqueAgentID(),
      orderID = transactionOpenerService.getAndIncSequentialID(partition),
      count = 0,
      isNotReliable = !isReliable)

    val msgCheckpointed = TransactionState(
      transactionID = transactionID,
      ttlMs = producerOptions.transactionTtlMs,
      status = TransactionState.Status.Checkpointed,
      partition = partition,
      masterID = -1,
      orderID = -1,
      count = data.size,
      isNotReliable = !isReliable)

    Seq(msgOpened, msgCheckpointed).foreach(subscriberNotifier.publish(_))
  }


  /**
    * Stop this agent
    */
  def stop() = {
    Producer.logger.info(s"Producer $name is shutting down.")

    if (isStop.getAndSet(true))
      throw new IllegalStateException(s"Producer ${this.name} is already stopped. Duplicate action.")

    openTransactionClient.stop()
    // stop provide master features to public
    transactionOpenerService.stop()

    // stop update state of all open transactions
    shutdownKeepAliveThread.signal(true)
    transactionKeepAliveThread.join()

    // stop executors
    asyncActivityService.shutdownOrDie(Producer.SHUTDOWN_WAIT_MAX_SECONDS, TimeUnit.SECONDS)
    notifyService.shutdownOrDie(Producer.SHUTDOWN_WAIT_MAX_SECONDS, TimeUnit.SECONDS)

    // stop function which works with subscribers
    subscriberNotifier.stop()
    curatorClient.close()
    stream.client.shutdown()
  }

  /**
    * Agent lock on any actions which has to do with checkpoint
    */
  override private[tstreams] def getThreadLock(): ReentrantLock = threadLock

  override private[tstreams] def getStorageClient(): StorageClient = stream.client
}