package com.bwsw.tstreams.agents.producer

import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.Executors

import com.bwsw.tstreams.agents.group.{Agent, CheckpointInfo, ProducerCheckpointInfo}
import com.bwsw.tstreams.agents.producer.ProducerPolicies.ProducerPolicy
import com.bwsw.tstreams.common.ThreadSignalSleepVar
import com.bwsw.tstreams.coordination.pubsub.SubscriberClient
import com.bwsw.tstreams.coordination.pubsub.messages.{ProducerTopicMessage, ProducerTransactionStatus}
import com.bwsw.tstreams.coordination.transactions.peertopeer.PeerToPeerAgent
import com.bwsw.tstreams.coordination.transactions.transport.traits.Interaction
import com.bwsw.tstreams.metadata.MetadataStorage
import com.bwsw.tstreams.streams.BasicStream
import org.slf4j.LoggerFactory

import scala.util.control.Breaks._

/**
 * Basic producer class
  *
  * @param name Producer name
 * @param stream Stream for transaction sending
 * @param producerOptions This producer options
 * @tparam USERTYPE User data type
 */
class BasicProducer[USERTYPE](val name : String,
                                       val stream : BasicStream[Array[Byte]],
                                       val producerOptions: BasicProducerOptions[USERTYPE])
  extends Agent with Interaction {

  // shortkey
  val pcs = producerOptions.producerCoordinationSettings

  // stores currently opened transactions per partition
  private val openTransactionsMap = scala.collection.mutable.Map[Int, BasicProducerTransaction[USERTYPE]]()
  private val logger              = LoggerFactory.getLogger(this.getClass)
  private val threadLock          = new ReentrantLock(true)

  // amount of threads which will handle partitions in masters, etc
  val thread_pool_size = {
    if (pcs.threadPoolAmount == -1)
      producerOptions.writePolicy.getUsedPartition().size
    else
      pcs.threadPoolAmount
  }

  stream.dataStorage.bind() //TODO: fix, probably deprecated

  logger.info(s"Start new Basic producer with name : $name, streamName : ${stream.getName}, streamPartitions : ${stream.getPartitions}\n")

  // this client is used to find new
  val subscriber_client = new SubscriberClient(
                                  prefix              = pcs.zkRootPath,
                                  streamName          = stream.getName,
                                  usedPartitions      = producerOptions.writePolicy.getUsedPartition(),
                                  zkHosts             = pcs.zkHosts,
                                  zkSessionTimeout    = pcs.zkSessionTimeout,
                                  zkConnectionTimeout = pcs.zkConnectionTimeout)

  /**
    * P2P Agent for producers interaction
    * (getNewTxn uuid; publish openTxn event; publish closeTxn event)
    */
  override val master_p2p_agent: PeerToPeerAgent = new PeerToPeerAgent(
                                              agentAddress        = pcs.agentAddress,
                                              zkHosts             = pcs.zkHosts,
                                              zkRootPath          = pcs.zkRootPath,
                                              zkSessionTimeout    = pcs.zkSessionTimeout,
                                              zkConnectionTimeout = pcs.zkConnectionTimeout,
                                              producer            = this,
                                              usedPartitions      = producerOptions.writePolicy.getUsedPartition(),
                                              isLowPriorityToBeMaster = pcs.isLowPriorityToBeMaster,
                                              transport               = pcs.transport,
                                              transportTimeout        = pcs.transportTimeout,
                                              poolSize                = thread_pool_size)

  //used for managing new agents on stream

  {
    val zkStreamLock = subscriber_client.getStreamLock(stream.getName)
    zkStreamLock.lock()
    subscriber_client.init()
    zkStreamLock.unlock()
  }


  /**
    * Queue to figure out moment when transaction is going to close
    */
  private val endKeepAliveThread  = new ThreadSignalSleepVar[Boolean](10)
  private val txnKeepAliveThread = getTxnKeepAliveThread

  /**
    *
    */
  def getTxnKeepAliveThread: Thread = {
    val latch                       = new CountDownLatch(1)
    val svc                         = Executors.newSingleThreadExecutor()
    val txnKeepAliveThread          = new Thread(new Runnable {
      override def run(): Unit = {
        latch.countDown()
        logger.info("Producer object is started. Launched open transaction update thread")
        breakable {
          while (true) {
            val value: Boolean = endKeepAliveThread.wait(producerOptions.transactionKeepAliveInterval * 1000)
            if (value) {
              logger.info("Producer object either checkpointed or cancelled. Exit KeepAliveThread.")
              svc.shutdownNow()
              break()
            }
            svc.submit(new Runnable { override def run(): Unit = updateOpenedTxns() })
          }
        }
      }
    })
    txnKeepAliveThread.start()
    latch.await()
    txnKeepAliveThread
  }
  /**
    *
    */
  def updateOpenedTxns() = {
    logger.info("Update event scheduled for long lasting transactions")
    openTransactionsMap.
      map { case(partition,txn)=>txn }.
      foreach { x=> if (!x.isClosed) x.updateTxnKeepAliveState() }
  }

  /**
   * @param policy Policy for previous transaction on concrete partition
   * @param nextPartition Next partition to use for transaction (default -1 which mean that write policy will be used)
   * @return BasicProducerTransaction instance
   */
  def newTransaction(policy: ProducerPolicy, nextPartition : Int = -1) : BasicProducerTransaction[USERTYPE] = {
    threadLock.lock()

    val partition = {
      if (nextPartition == -1)
        producerOptions.writePolicy.getNextPartition
      else
        nextPartition
    }

    if (!(partition >= 0 && partition < stream.getPartitions))
      throw new IllegalArgumentException("invalid partition")

    val transaction = {
      val txnUUID = master_p2p_agent.getNewTxn(partition)
      logger.debug(s"[NEW_TRANSACTION PARTITION_$partition] uuid=${txnUUID.timestamp()}\n")
      if (openTransactionsMap.contains(partition)) {
        val prevTxn = openTransactionsMap(partition)
        if (!prevTxn.isClosed) {
          policy match {
            case ProducerPolicies.`checkpointIfOpened` =>
              prevTxn.checkpoint()

            case ProducerPolicies.`cancelIfOpened` =>
              prevTxn.cancel()

            case ProducerPolicies.`errorIfOpened` =>
              throw new IllegalStateException("previous transaction was not closed")
          }
        }
      }

      val txn = new BasicProducerTransaction[USERTYPE](threadLock, partition, txnUUID, this)
      openTransactionsMap(partition) = txn
      txn
    }
    threadLock.unlock()
    transaction
  }

  /**
   * Return reference on transaction from concrete partition
    *
    * @param partition Partition from which transaction will be retrieved
   * @return Transaction reference if it exist or not closed
   */
  def getOpenTransactionForPartition(partition : Int) : Option[BasicProducerTransaction[USERTYPE]] = {
    threadLock.lock()

    if (!(partition >= 0 && partition < stream.getPartitions))
      throw new IllegalArgumentException("invalid partition")

    val res = if (openTransactionsMap.contains(partition)) {
      val txn = openTransactionsMap(partition)
      if (txn.isClosed)
        return None
      Some(txn)
    }
    else
      None
    threadLock.unlock()

    res
  }

  /**
   * Close all opened transactions
   */
  def checkpoint() : Unit = {
    threadLock.lock()

    openTransactionsMap.
        map { case(partition,txn)=>txn }.
          foreach { x=> if (!x.isClosed) x.checkpoint() }

    threadLock.unlock()
  }

  /**
   * Info to commit
   */
  override def getCheckpointInfoAndClear(): List[CheckpointInfo] = {
    threadLock.lock()
    val checkpointData = openTransactionsMap.map {
      case (partition, txn) =>
          assert(partition == txn.getPartition)

          val preCheckpoint = ProducerTopicMessage(
                                      txnUuid = txn.getTxnUUID,
                                      ttl = -1,
                                      status = ProducerTransactionStatus.preCheckpoint,
                                      partition = partition)

          val finalCheckpoint = ProducerTopicMessage(
                                      txnUuid = txn.getTxnUUID,
                                      ttl = -1,
                                      status = ProducerTransactionStatus.postCheckpoint,
                                      partition = partition)

          ProducerCheckpointInfo(transactionRef        = txn,
                                  agent                 = master_p2p_agent,
                                  preCheckpointEvent    = preCheckpoint,
                                  finalCheckpointEvent  = finalCheckpoint,
                                  streamName            = stream.getName,
                                  partition             = partition,
                                  transaction           = txn.getTxnUUID,
                                  totalCnt              = txn.getCnt,
                                  ttl                   = stream.getTTL) }.filter(pci => pci.partition > 0).toList

    openTransactionsMap.clear()
    threadLock.unlock()
    checkpointData
  }

  /**
   * @return Metadata storage link for concrete agent
   */
  override def getMetadataRef(): MetadataStorage =
    stream.metadataStorage

  /**
    *
    * @return
    */
  def getNewTxnUUIDLocal(): UUID = {
    val transactionUuid = producerOptions.txnGenerator.getTimeUUID()
    transactionUuid
  }

  /**
   * Method to implement for concrete producer [[PeerToPeerAgent]] method
   * Need only if this producer is master
    *
    * @return UUID
   */
  override def openTxnLocal(txnUUID : UUID, partition : Int, onComplete: () => Unit) : Unit = {
    stream.metadataStorage.commitEntity.commit(
                                streamName  = stream.getName,
                                partition   = partition,
                                transaction = txnUUID,
                                totalCnt    = -1,
                                ttl         = producerOptions.transactionTTL)

    val msg = ProducerTopicMessage(
                                txnUuid     = txnUUID,
                                ttl         = producerOptions.transactionTTL,
                                status      = ProducerTransactionStatus.opened,
                                partition   = partition)

    logger.debug(s"[GET_LOCAL_TXN PRODUCER] update with msg partition=$partition uuid=${txnUUID.timestamp()} opened")
    subscriber_client.publish(msg, onComplete)
  }


  /**
   * Stop this agent
   */
  def stop() = {
    threadLock.lock()

    endKeepAliveThread.signal(true)
    txnKeepAliveThread.join()

    master_p2p_agent.stop()
    subscriber_client.stop()

    threadLock.unlock()
  }

  /**
    * Agent lock on any actions which has to do with checkpoint
    */
  override def getThreadLock(): ReentrantLock = threadLock

}