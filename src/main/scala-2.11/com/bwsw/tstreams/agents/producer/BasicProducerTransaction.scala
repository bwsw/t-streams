package com.bwsw.tstreams.agents.producer

import java.util.UUID
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, TimeUnit}

import com.bwsw.tstreams.common.ThreadSignalSleepVar
import com.bwsw.tstreams.coordination.pubsub.messages.{ProducerTopicMessage, ProducerTransactionStatus}
import com.bwsw.tstreams.debug.GlobalHooks
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

/**
 * Transaction retrieved by BasicProducer.newTransaction method
 *
 * @param threadLock Producer Lock for managing actions which has to do with checkpoints
 * @param partition Concrete partition for saving this transaction
 * @param txnOwner Producer class which was invoked newTransaction method
 * @param transactionUuid UUID for this transaction
 * @tparam USERTYPE User data type
 */
class BasicProducerTransaction[USERTYPE](threadLock       : ReentrantLock,
                                         partition        : Int,
                                         transactionUuid  : UUID,
                                         txnOwner    : BasicProducer[USERTYPE]){

  /**
    * State indicator of the transaction
    *
    * @return Closed transaction or not
    */
  def isClosed = closed

  /**
   * BasicProducerTransaction logger for logging
   */
  private val logger = LoggerFactory.getLogger(this.getClass)
  logger.debug(s"Open transaction for stream,partition : {${txnOwner.stream.getName}},{$partition}\n")

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
  private val endKeepAliveThread = new ThreadSignalSleepVar[Boolean](1)

  /**
   * Thread to keep this transaction alive
   */
  //private val keepAliveThread: Thread = startAsyncKeepAlive()

  /**
   * Send data to storage
    *
    * @param obj some user object
   */
  def send(obj : USERTYPE) : Unit = {
    threadLock.lock()

    if (closed)
      throw new IllegalStateException("transaction is closed")

    txnOwner.producerOptions.insertType match {

      case InsertionType.BatchInsert(size) =>

        txnOwner.stream.dataStorage.putInBuffer(
                                            txnOwner.stream.getName,
                                            partition,
                                            transactionUuid,
                                            txnOwner.stream.getTTL,
                                            txnOwner.producerOptions.converter.convert(obj),
                                            part)

        if (txnOwner.stream.dataStorage.getBufferSize(transactionUuid) == size) {

          val job: () => Unit = txnOwner.stream.dataStorage.saveBuffer(transactionUuid)
          if (job != null) jobs += job
          txnOwner.stream.dataStorage.clearBuffer(transactionUuid)

        }

      case InsertionType.SingleElementInsert =>

        val job: () => Unit = txnOwner.stream.dataStorage.put(
                                            txnOwner.stream.getName,
                                            partition,
                                            transactionUuid,
                                            txnOwner.stream.getTTL,
                                            txnOwner.producerOptions.converter.convert(obj),
                                            part)
        if (job != null) jobs += job
    }

    part += 1
    threadLock.unlock()
  }

  /**
   * Canceling current transaction
   */
  def cancel() = {
    threadLock.lock()

    if (closed)
      throw new IllegalStateException("transaction is already closed")

    stopKeepAlive()

    txnOwner.producerOptions.insertType match {
      case InsertionType.SingleElementInsert =>

      case InsertionType.BatchInsert(_) =>
        txnOwner.stream.dataStorage.clearBuffer(transactionUuid)
    }

    threadLock.unlock()

    val msg = ProducerTopicMessage(txnUuid   = transactionUuid,
                                    ttl       = -1,
                                    status    = ProducerTransactionStatus.cancel,
                                    partition = partition)

    txnOwner.master_p2p_agent.publish(msg)
    logger.debug(s"[CANCEL PARTITION_${msg.partition}] ts=${msg.txnUuid.timestamp()} status=${msg.status}")

  }

  def stopKeepAlive() = {
    // wait all async jobs completeness before commit
    jobs.foreach(x => x())

    // signal keep alive thread to stop
    //endKeepAliveThread.signal(true)

    //await till update thread will be stoped
    //keepAliveThread.join()

    closed = true
  }

  /**
   * Submit transaction(transaction will be available by consumer only after closing)
   */
  def checkpoint() : Unit = {
    threadLock.lock()

    if (closed)
      throw new IllegalStateException("transaction is already closed")

    txnOwner.producerOptions.insertType match {

      case InsertionType.SingleElementInsert =>

      case InsertionType.BatchInsert(size) =>
        if (txnOwner.stream.dataStorage.getBufferSize(transactionUuid) > 0) {
          val job: () => Unit = txnOwner.stream.dataStorage.saveBuffer(transactionUuid)
          if (job != null) jobs += job
          txnOwner.stream.dataStorage.clearBuffer(transactionUuid)
        }
    }

    //close transaction using stream ttl
    if (part > 0) {

      txnOwner.master_p2p_agent.publish(ProducerTopicMessage(
                                          txnUuid   = transactionUuid,
                                          ttl       = -1,
                                          status    = ProducerTransactionStatus.preCheckpoint,
                                          partition = partition))

      logger.debug(s"[PRE CHECKPOINT PARTITION_$partition] " +
        s"ts=${transactionUuid.timestamp()}")

      //must do it after agent.publish cuz it can be long operation
      //because of agents re-election
      stopKeepAlive()

      //debug purposes only
      GlobalHooks.invoke("PreCommitFailure")

      txnOwner.stream.metadataStorage.commitEntity.commit(
                                      streamName  = txnOwner.stream.getName,
                                      partition   = partition,
                                      transaction = transactionUuid,
                                      totalCnt    = part,
                                      ttl         = txnOwner.stream.getTTL)

      logger.debug(s"[COMMIT PARTITION_$partition] ts=${transactionUuid.timestamp()}")

      //debug purposes only
      GlobalHooks.invoke("AfterCommitFailure")

      txnOwner.master_p2p_agent.publish(ProducerTopicMessage(
                                              txnUuid   = transactionUuid,
                                              ttl       = -1,
                                              status    = ProducerTransactionStatus.postCheckpoint,
                                              partition = partition))

      logger.debug(s"[FINAL CHECKPOINT PARTITION_$partition] " +
        s"ts=${transactionUuid.timestamp()}")
    }
    else {
      txnOwner.master_p2p_agent.publish(ProducerTopicMessage(
                                            txnUuid   = transactionUuid,
                                            ttl       = -1,
                                            status    = ProducerTransactionStatus.cancel,
                                            partition = partition))
      stopKeepAlive()
    }
    threadLock.unlock()
  }


  /**
   * Async job for keeping alive current transaction
   */
  private def startAsyncKeepAlive() : Thread = {
    val latch              = new CountDownLatch(1)
    val txnKeepAliveThread = new Thread(new Runnable {
      override def run(): Unit = {
        latch.countDown()
        logger.debug(s"[START KEEP_ALIVE THREAD PARTITION=$partition UUID=${transactionUuid.timestamp()}")
        breakable {
          while (true) {
            val value: Boolean = endKeepAliveThread.wait(txnOwner.producerOptions.transactionKeepAliveInterval * 1000)
            if (value) {
              logger.info("Transaction object either checkpointed or cancelled. Exit KeepAliveThread.")
              break()
            }
            updateTxnKeepAliveState()
          }
        }
      }
    })
    txnKeepAliveThread.start()
    latch.await()
    txnKeepAliveThread
  }

  def updateTxnKeepAliveState() = {
    //-1 here indicate that transaction is started but is not finished yet
    txnOwner.stream.metadataStorage.commitEntity.commit(
      streamName      = txnOwner.stream.getName,
      partition       = partition,
      transaction     = transactionUuid,
      totalCnt        = -1,
      ttl             = txnOwner.producerOptions.transactionTTL)


    //publish that current txn is being updating
    txnOwner.subscriber_client.publish(ProducerTopicMessage(
      txnUuid   = transactionUuid,
      ttl       = txnOwner.producerOptions.transactionTTL,
      status    = ProducerTransactionStatus.update,
      partition = partition), ()=>())
    logger.debug(s"[KEEP_ALIVE THREAD PARTITION_${partition}] ts=${transactionUuid.timestamp()} status=${ProducerTransactionStatus.update}")
  }
}