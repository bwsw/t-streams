package com.bwsw.tstreams.agents.producer

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.group.{CheckpointInfo, GroupParticipant, SendingAgent}
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy.ProducerPolicy
import com.bwsw.tstreams.common._
import com.bwsw.tstreams.coordination.client.BroadcastCommunicationClient
import com.bwsw.tstreams.coordination.messages.state.{TransactionStateMessage, TransactionStatus}
import com.bwsw.tstreams.streams.{Stream}
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.KeeperException
import org.slf4j.LoggerFactory

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
  override def getAgentName() = name

  def setAgentName(name: String) = {
    this.name = name
  }

  // short key
  val pcs = producerOptions.coordinationOptions
  val isStop = new AtomicBoolean(false)

  private val openTransactions = new OpenTransactionsKeeper()

  // stores latches for materialization await (protects from materialization before main transaction response)
  private val materializationGovernor = new MaterializationGovernor(producerOptions.writePolicy.getUsedPartitions())
  private val threadLock = new ReentrantLock(true)

  private val peerKeepAliveTimeout = pcs.zkSessionTimeoutMs * 2

  val fullPrefix = java.nio.file.Paths.get(pcs.zkPrefix, stream.name).toString.substring(1)
  private val curatorClient = CuratorFrameworkFactory.builder()
    .namespace(fullPrefix)
    .connectionTimeoutMs(pcs.zkConnectionTimeoutMs)
    .sessionTimeoutMs( pcs.zkSessionTimeoutMs)
    .retryPolicy(new ExponentialBackoffRetry(1000, 3))
    .connectString(pcs.zkEndpoints).build()

  curatorClient.start()

  try {
    curatorClient.create().creatingParentContainersIfNeeded().forPath("/subscribers")
  } catch {
    case e: KeeperException =>
      if(e.code() != KeeperException.Code.NODEEXISTS)
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
  val subscriberNotifier = new BroadcastCommunicationClient(curatorClient, partitions = producerOptions.writePolicy.getUsedPartitions())
  subscriberNotifier.init()

  /**
    * P2P Agent for producers interaction
    * (getNewTransaction id; publish openTransaction event; publish closeTransaction event)
    */
  override val p2pAgent: PeerAgent = new PeerAgent(
    curatorClient           = curatorClient,
    peerKeepAliveTimeout    = peerKeepAliveTimeout,
    producer                = this,
    usedPartitions          = producerOptions.writePolicy.getUsedPartitions(),
    transport               = pcs.transport,
    threadPoolAmount        = threadPoolSize,
    threadPoolPublisherThreadsAmount  = pcs.notifyThreadPoolSize,
    partitionRedistributionDelay      = pcs.partitionRedistributionDelaySec)


  /**
    * Queue to figure out moment when transaction is going to close
    */
  private val shutdownKeepAliveThread = new ThreadSignalSleepVar[Boolean](1)
  private val transactionKeepAliveThread = getTransactionKeepAliveThread
  val backendActivityService = new FirstFailLockableTaskExecutor(s"Producer $name-BackendWorker")
  val asyncActivityService = new FirstFailLockableTaskExecutor(s"Producer $name-AsyncWorker")


  /**
    * Allows to get if the producer is master for the partition.
    *
    * @param partition
    * @return
    */
  def isMasterOfPartition(partition: Int): Boolean =
    p2pAgent.isMasterOfPartition(partition)

  def getPartitionMasterIDLocalInfo(partition: Int): Int =
    p2pAgent.getPartitionMasterInetAddressLocal(partition)._2

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

  private def newTransactionUnsafe(policy: ProducerPolicy, partition: Int = -1): ProducerTransaction = {
    if (isStop.get())
      throw new IllegalStateException(s"Producer ${this.name} is already stopped. Unable to get new transaction.")

    val p = {
      if (partition == -1)
        producerOptions.writePolicy.getNextPartition
      else
        partition
    }

    if (!(p >= 0 && p < stream.partitionsCount))
      throw new IllegalArgumentException(s"Producer $name - invalid partition")

    val previousTransactionAction: () => Unit =
      openTransactions.awaitOpenTransactionMaterialized(p, policy)

    materializationGovernor.protect(p)


    val transactionID = p2pAgent.generateNewTransaction(p)
    if (Producer.logger.isDebugEnabled)
      Producer.logger.debug(s"[NEW_TRANSACTION PARTITION_$p] ID=$transactionID")
    val transaction = new ProducerTransaction(p, transactionID, this)

    openTransactions.put(p, transaction)
    materializationGovernor.unprotect(p)

    if (previousTransactionAction != null)
      asyncActivityService.submit("<PreviousTransactionActionTask>", () => previousTransactionAction())
    transaction
  }

  /**
    * @param policy    Policy for previous transaction on concrete partition
    * @param partition Next partition to use for transaction (default -1 which mean that write policy will be used)
    * @return BasicProducerTransaction instance
    */
  def newTransaction(policy: ProducerPolicy, partition: Int = -1, retry: Int = 1): ProducerTransaction = {
    if (isStop.get())
      throw new IllegalStateException(s"Producer ${this.name} is already stopped. Unable to get new transaction.")

    if(retry < 0)
      throw new IllegalStateException("Failed to get a new transaction.")

    try {
      val transaction = newTransactionUnsafe(policy, partition)
      transaction.awaitMaterialized()
      transaction
    } catch {
      case e: MaterializationException =>
        newTransaction(policy, retry - 1)
    }
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
  override def getCheckpointInfoAndClear(): List[CheckpointInfo] = {
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
  override def openTransactionLocal(transactionID: Long, partition: Int, onComplete: () => Unit): Unit = {

    p2pAgent.submitPipelinedTaskToPublishExecutors(partition, () => {
      val msg = TransactionStateMessage(
        transactionID = transactionID,
        ttl = producerOptions.transactionTtlMs,
        status = TransactionStatus.opened,
        partition = partition,
        masterID = p2pAgent.getUniqueAgentID(),
        orderID = p2pAgent.getAndIncSequentialID(partition),
        count = 0)
      subscriberNotifier.publish(msg)
      if (Producer.logger.isDebugEnabled)
        Producer.logger.debug(s"Producer $name - [GET_LOCAL_TRANSACTION] update with message partition=$partition ID=$transactionID opened")
    })

    val transactionRecord = new RPCProducerTransaction(stream.name, partition, transactionID, TransactionStates.Opened, -1, producerOptions.transactionTtlMs)

    stream.client.putTransaction(transactionRecord, true)(rec => {
      p2pAgent.submitPipelinedTaskToMaterializeExecutor(partition, onComplete)
    })

  }


  /**
    * Stop this agent
    */
  def stop() = {
    Producer.logger.info(s"Producer $name is shutting down.")

    if (isStop.getAndSet(true))
      throw new IllegalStateException(s"Producer ${this.name} is already stopped. Duplicate action.")

    // stop update state of all open transactions
    shutdownKeepAliveThread.signal(true)
    transactionKeepAliveThread.join()
    // stop executor

    asyncActivityService.shutdownOrDie(Producer.SHUTDOWN_WAIT_MAX_SECONDS, TimeUnit.SECONDS)

    // TODO: fixit
    Thread.sleep(1000)
//    while (tsdb.getResourceCounter() != 0) {
//      Producer.logger.info(s"Waiting for all database async callbacks will be executed. Pending: ${tsdb.getResourceCounter()}.")
//      Thread.sleep(200)
//    }
    backendActivityService.shutdownOrDie(Producer.SHUTDOWN_WAIT_MAX_SECONDS, TimeUnit.SECONDS)

    // stop provide master features to public
    p2pAgent.stop()
    // stop function which works with subscribers
    subscriberNotifier.stop()
    curatorClient.close()
  }

  /**
    * Agent lock on any actions which has to do with checkpoint
    */
  override def getThreadLock(): ReentrantLock = threadLock


  /**
    * Special method which waits until newTransaction method will be completed and after
    * does materialization. It's called from another thread (p2pAgent), not from thread of
    * Producer.
    *
    * @param msg
    */
  def materialize(msg: TransactionStateMessage):Unit = {

    if (Producer.logger.isDebugEnabled)
      Producer.logger.debug(s"Start handling MaterializeRequest at partition: ${msg.partition}")

    materializationGovernor.awaitUnprotected(msg.partition)
    val opt = getOpenedTransactionForPartition(msg.partition)

    if(opt.isEmpty) {
      Producer.logger.warn(s"There is no opened transaction for ${msg.partition}.")
      return
    }

    if (Producer.logger.isDebugEnabled)
      Producer.logger.debug(s"In Map Transaction: ${opt.get.getTransactionID.toString}\nIn Request Transaction: ${msg.transactionID}")

    if(!(opt.get.getTransactionID == msg.transactionID && msg.status == TransactionStatus.materialize)) {
      Producer.logger.warn(s"Materialization is requested for transaction ${msg.transactionID} but expected transaction is ${opt.get.getTransactionID}.")
      opt.get.markAsClosed()
      return
    }

    opt.get.makeMaterialized()
    if (Producer.logger.isDebugEnabled)
      Producer.logger.debug("End handling MaterializeRequest")

  }

  override def getStorageClient(): StorageClient = stream.client
}