package com.bwsw.tstreams.agents.producer

import java.util.UUID
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, TimeUnit}

import com.bwsw.tstreams.coordination.pubsub.messages.{ProducerTopicMessage, ProducerTransactionStatus}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

/**
 * Transaction retrieved by BasicProducer.newTransaction method
 * @param producerLock Producer Lock for managing actions which has to do with checkpoints
 * @param partition Concrete partition for saving this transaction
 * @param basicProducer Producer class which was invoked newTransaction method
 * @param transactionUuid UUID for this transaction
 * @tparam USERTYPE User data type
 * @tparam DATATYPE Storage data type
 */
class BasicProducerTransaction[USERTYPE,DATATYPE](producerLock: ReentrantLock,
                                                  partition : Int,
                                                  transactionUuid : UUID,
                                                  basicProducer: BasicProducer[USERTYPE,DATATYPE]){

  /**
   * BasicProducerTransaction logger for logging
   */
  private val logger = LoggerFactory.getLogger(this.getClass)
  logger.debug(s"Open transaction for stream,partition : {${basicProducer.stream.getName}},{$partition}\n")

  /**
   * Return transaction partition
   */
  def getPartition : Int = partition

  /**
   * Return transaction UUID
   */
  def getTxnUUID: UUID = transactionUuid

  /**
   * Return current transaction amount of data
   */
  def getCnt = part

  /**
   * Variable for indicating transaction state
   */
  private var closed = false

  /**
   * Transaction part index
   */
  private var part = 0

  /**
   * All inserts (can be async) in storage (must be waited before closing this transaction)
   */
  private var jobs = ListBuffer[() => Unit]()

  /**
   * Queue to figure out moment when transaction is going to close
   */
  private val updateQueue = new LinkedBlockingQueue[Boolean](10)

  /**
   * Thread to keep this transaction alive
   */
  private val updateThread: Thread = startAsyncKeepAlive()

  /**
   * Send data to storage
 *
   * @param obj some user object
   */
  def send(obj : USERTYPE) : Unit = {
    producerLock.lock()
    if (closed)
      throw new IllegalStateException("transaction is closed")

    basicProducer.producerOptions.insertType match {
      case InsertionType.BatchInsert(size) =>
        basicProducer.stream.dataStorage.putInBuffer(
          basicProducer.stream.getName,
          partition,
          transactionUuid,
          basicProducer.stream.getTTL,
          basicProducer.producerOptions.converter.convert(obj),
          part)
        if (basicProducer.stream.dataStorage.getBufferSize(transactionUuid) == size) {
          val job: () => Unit = basicProducer.stream.dataStorage.saveBuffer(transactionUuid)
          if (job != null)
            jobs += job
          basicProducer.stream.dataStorage.clearBuffer(transactionUuid)
        }

      case InsertionType.SingleElementInsert =>
        val job: () => Unit = basicProducer.stream.dataStorage.put(
          basicProducer.stream.getName,
          partition,
          transactionUuid,
          basicProducer.stream.getTTL,
          basicProducer.producerOptions.converter.convert(obj),
          part)
        if (job != null)
          jobs += job

      case _ =>
        throw new IllegalStateException("InsertType can't be resolved")
    }

    part += 1
    producerLock.unlock()
  }

  /**
   * Canceling current transaction
   */
  def cancel() = {
    producerLock.lock()
    if (closed)
      throw new IllegalStateException("transaction is already closed")

    basicProducer.producerOptions.insertType match {
      case InsertionType.SingleElementInsert =>

      case InsertionType.BatchInsert(_) =>
        basicProducer.stream.dataStorage.clearBuffer(transactionUuid)

      case _ =>
        throw new IllegalStateException("Insert Type can't be resolved")
    }

    stopKeepAlive()

    val msg = ProducerTopicMessage(txnUuid = transactionUuid,
      ttl = -1, status = ProducerTransactionStatus.cancelled, partition = partition)

    basicProducer.agent.publish(msg)
    logger.debug(s"[CANCEL PARTITION_${msg.partition}] ts=${msg.txnUuid.timestamp()} status=${msg.status}")

    closed = true
    producerLock.unlock()
  }

  def stopKeepAlive() = {
    // wait all async jobs completeness before commit
    jobs.foreach(x => x())

    updateQueue.put(true)

    //await till update thread will be stoped
    updateThread.join()
  }

  /**
   * Submit transaction(transaction will be available by consumer only after closing)
   */
  def checkpoint() : Unit = {
    producerLock.lock()
    if (closed)
      throw new IllegalStateException("transaction is already closed")

    basicProducer.producerOptions.insertType match {
      case InsertionType.SingleElementInsert =>

      case InsertionType.BatchInsert(size) =>
        if (basicProducer.stream.dataStorage.getBufferSize(transactionUuid) > 0) {
          val job: () => Unit = basicProducer.stream.dataStorage.saveBuffer(transactionUuid)
          if (job != null)
            jobs += job
          basicProducer.stream.dataStorage.clearBuffer(transactionUuid)
        }

      case _ =>
        throw new IllegalStateException("Insert Type can't be resolved")
    }

    //close transaction using stream ttl
    if (part > 0) {
      val preCheckpoint = ProducerTopicMessage(
        txnUuid = transactionUuid,
        ttl = -1,
        status = ProducerTransactionStatus.preCheckpoint,
        partition = partition)
      basicProducer.agent.publish(preCheckpoint)

      logger.debug(s"[PRE CHECKPOINT PARTITION_$partition] " +
        s"ts=${transactionUuid.timestamp()}")

      //we must do it after agent.publish cuz it can be long operation
      //because of agents re-election
      stopKeepAlive()

      basicProducer.stream.metadataStorage.commitEntity.commit(
        streamName = basicProducer.stream.getName,
        partition = partition,
        transaction = transactionUuid,
        totalCnt = part,
        ttl = basicProducer.stream.getTTL)

      logger.debug(s"[COMMIT PARTITION_$partition] " +
        s"ts=${transactionUuid.timestamp()}")

      val finalCheckpoint = ProducerTopicMessage(
        txnUuid = transactionUuid,
        ttl = -1,
        status = ProducerTransactionStatus.finalCheckpoint,
        partition = partition)
      basicProducer.agent.publish(finalCheckpoint)

      logger.debug(s"[FINAL CHECKPOINT PARTITION_$partition] " +
        s"ts=${transactionUuid.timestamp()}")
    }
    else {
      stopKeepAlive()
    }

    closed = true
    producerLock.unlock()
  }

  /**
   * State indicator of the transaction
 *
   * @return Closed transaction or not
   */
  def isClosed = closed

  /**
   * Async job for keeping alive current transaction
   */
  private def startAsyncKeepAlive() : Thread = {
    val latch = new CountDownLatch(1)
    val updater = new Thread(new Runnable {
      override def run(): Unit = {
        latch.countDown()
        logger.debug(s"[START KEEP_ALIVE THREAD PARTITION=$partition UUID=${transactionUuid.timestamp()}")
        breakable { while (true) {
          val value = updateQueue.poll(basicProducer.producerOptions.transactionKeepAliveInterval * 1000, TimeUnit.MILLISECONDS)

          if (value)
            break()

          //-1 here indicate that transaction is started but is not finished yet
          basicProducer.stream.metadataStorage.producerCommitEntity.commit(
            streamName = basicProducer.stream.getName,
            partition = partition,
            transaction = transactionUuid,
            totalCnt = -1,
            ttl = basicProducer.producerOptions.transactionTTL)

          val msg = ProducerTopicMessage(
            txnUuid = transactionUuid,
            ttl = basicProducer.producerOptions.transactionTTL,
            status = ProducerTransactionStatus.updated,
            partition = partition)

          //publish that current txn is being updating
          basicProducer.coordinator.publish(msg)
          logger.debug(s"[KEEP_ALIVE THREAD PARTITION_${msg.partition}] ts=${msg.txnUuid.timestamp()} status=${msg.status}")
        }}
      }
    })
    updater.start()
    latch.await()
    updater
  }
}