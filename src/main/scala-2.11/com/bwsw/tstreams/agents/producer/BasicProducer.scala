package com.bwsw.tstreams.agents.producer

import java.util.UUID
import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreams.agents.group.{Agent, CommitInfo, ProducerCommitInfo}
import com.bwsw.tstreams.agents.producer.ProducerPolicies.ProducerPolicy
import com.bwsw.tstreams.coordination.pubsub.ProducerCoordinator
import com.bwsw.tstreams.coordination.pubsub.messages.{ProducerTopicMessage, ProducerTransactionStatus}
import com.bwsw.tstreams.coordination.transactions.peertopeer.PeerToPeerAgent
import com.bwsw.tstreams.coordination.transactions.transport.traits.Interaction
import com.bwsw.tstreams.metadata.MetadataStorage
import com.bwsw.tstreams.streams.BasicStream
import org.slf4j.LoggerFactory

/**
 * Basic producer class
  *
  * @param name Producer name
 * @param stream Stream for transaction sending
 * @param producerOptions This producer options
 * @tparam USERTYPE User data type
 * @tparam DATATYPE Storage data type
 */
class BasicProducer[USERTYPE,DATATYPE](val name : String,
                                       val stream : BasicStream[DATATYPE],
                                       val producerOptions: BasicProducerOptions[USERTYPE,DATATYPE])
  extends Agent with Interaction{

  stream.dataStorage.bind()
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val producerLock = new ReentrantLock(true)

  logger.info(s"Start new Basic producer with name : $name, streamName : ${stream.getName}, streamPartitions : ${stream.getPartitions}\n")

  private val partitionToTransaction = scala.collection.mutable.Map[Int, BasicProducerTransaction[USERTYPE,DATATYPE]]()

  val coordinator = new ProducerCoordinator(
    prefix = producerOptions.producerCoordinationSettings.zkRootPath,
    streamName = stream.getName,
    usedPartitions = producerOptions.writePolicy.getUsedPartition(),
    zkHosts = producerOptions.producerCoordinationSettings.zkHosts,
    zkSessionTimeout = producerOptions.producerCoordinationSettings.zkSessionTimeout,
    zkConnectionTimeout = producerOptions.producerCoordinationSettings.zkConnectionTimeout)

  private val streamLock = coordinator.getStreamLock(stream.getName)
  //used for managing new agents on stream
  streamLock.lock()
  coordinator.init()
  streamLock.unlock()

  /**
   * @param policy Policy for previous transaction on concrete partition
   * @param nextPartition Next partition to use for transaction (default -1 which mean that write policy will be used)
   * @return BasicProducerTransaction instance
   */
  def newTransaction(policy: ProducerPolicy, nextPartition : Int = -1) : BasicProducerTransaction[USERTYPE,DATATYPE] = {
    producerLock.lock()
    val partition = {
      if (nextPartition == -1)
        producerOptions.writePolicy.getNextPartition
      else
        nextPartition
    }

    if (!(partition >= 0 && partition < stream.getPartitions))
      throw new IllegalArgumentException("invalid partition")

    val transaction = {
      val txnUUID = agent.getNewTxn(partition)
      logger.debug(s"[NEW_TRANSACTION PARTITION_$partition] uuid=${txnUUID.timestamp()}\n")
      if (partitionToTransaction.contains(partition)) {
        val prevTxn = partitionToTransaction(partition)
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
      val txn = new BasicProducerTransaction[USERTYPE, DATATYPE](producerLock, partition, txnUUID, this)
      partitionToTransaction(partition) = txn
      txn
    }
    producerLock.unlock()
    transaction
  }

  /**
   * Return reference on transaction from concrete partition
    *
    * @param partition Partition from which transaction will be retrieved
   * @return Transaction reference if it exist or not closed
   */
  def getTransaction(partition : Int) : Option[BasicProducerTransaction[USERTYPE,DATATYPE]] = {
    producerLock.lock()
    if (!(partition >= 0 && partition < stream.getPartitions))
      throw new IllegalArgumentException("invalid partition")
    val res = if (partitionToTransaction.contains(partition)) {
      val txn = partitionToTransaction(partition)
      if (txn.isClosed)
        return None
      Some(txn)
    }
    else
      None
    producerLock.unlock()
    res
  }

  /**
   * Close all opened transactions
   */
  def checkpoint() : Unit = {
    producerLock.lock()
    partitionToTransaction.map{case(partition,txn)=>txn}.foreach{ x=>
      if (!x.isClosed)
        x.checkpoint()
    }
    producerLock.unlock()
  }

  /**
   * Info to commit
   */
  override def getCommitInfo(): List[CommitInfo] = {
    val checkpointData = partitionToTransaction.map{ case (partition, txn) =>
      assert(partition == txn.getPartition)
      val preCheckpoint = ProducerTopicMessage(
        txnUuid = txn.getTxnUUID,
        ttl = -1,
        status = ProducerTransactionStatus.preCheckpoint,
        partition = partition)
      val finalCheckpoint = ProducerTopicMessage(
        txnUuid = txn.getTxnUUID,
        ttl = -1,
        status = ProducerTransactionStatus.finalCheckpoint,
        partition = partition)
      ProducerCommitInfo(
        transactionRef = txn,
        agent = agent,
        preCheckpointEvent = preCheckpoint,
        finalCheckpointEvent = finalCheckpoint,
        streamName = stream.getName,
        partition = partition,
        transaction = txn.getTxnUUID,
        totalCnt = txn.getCnt,
        ttl = stream.getTTL)
    }.toList
    partitionToTransaction.clear()
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
  def getTransactionUUID(): UUID = {
    val transactionUuid = producerOptions.txnGenerator.getTimeUUID()
    transactionUuid
  }

  /**
   * Method to implement for concrete producer [[PeerToPeerAgent]] method
   * Need only if this producer is master
    *
    * @return UUID
   */
  override def commitLocalTxn(txnUUID : UUID, partition : Int, onComplete: () => Unit) : Unit = {
    stream.metadataStorage.commitEntity.commit(
      streamName = stream.getName,
      partition = partition,
      transaction = txnUUID,
      totalCnt = -1,
      ttl = producerOptions.transactionTTL)

    val msg = ProducerTopicMessage(
      txnUuid = txnUUID,
      ttl = producerOptions.transactionTTL,
      status = ProducerTransactionStatus.opened,
      partition = partition)

    logger.debug(s"[GET_LOCAL_TXN PRODUCER] update with msg partition=$partition uuid=${txnUUID.timestamp()} opened")
    coordinator.publish(msg, onComplete)
  }

  /**
   * P2P Agent for producers interaction
   * (getNewTxn uuid; publish openTxn event; publish closeTxn event)
   */
  override val agent: PeerToPeerAgent = new PeerToPeerAgent(
    agentAddress = producerOptions.producerCoordinationSettings.agentAddress,
    zkHosts = producerOptions.producerCoordinationSettings.zkHosts,
    zkRootPath = producerOptions.producerCoordinationSettings.zkRootPath,
    zkSessionTimeout = producerOptions.producerCoordinationSettings.zkSessionTimeout,
    zkConnectionTimeout = producerOptions.producerCoordinationSettings.zkConnectionTimeout,
    producer = this,
    usedPartitions = producerOptions.writePolicy.getUsedPartition(),
    isLowPriorityToBeMaster = producerOptions.producerCoordinationSettings.isLowPriorityToBeMaster,
    transport = producerOptions.producerCoordinationSettings.transport,
    transportTimeout = producerOptions.producerCoordinationSettings.transportTimeout,
    poolSize = if (producerOptions.producerCoordinationSettings.threadPoolAmount == -1)
                  producerOptions.writePolicy.getUsedPartition().size
               else
                  producerOptions.producerCoordinationSettings.threadPoolAmount)

  /**
   * Stop this agent
   */
  def stop() = {
    agent.stop()
    coordinator.stop()
  }

  /**
    * Agent lock on any actions which has to do with checkpoint
    */
  override def getAgentLock(): ReentrantLock =
    producerLock

}