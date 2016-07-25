package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.UUID
import java.util.concurrent.ExecutorService
import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreams.coordination.pubsub.SubscriberCoordinator
import com.bwsw.tstreams.coordination.pubsub.messages.{ProducerTopicMessage, ProducerTransactionStatus}
import ProducerTransactionStatus._
import com.bwsw.tstreams.txnqueue.PersistentTransactionQueue
import org.slf4j.LoggerFactory

import scala.util.control.Breaks._


/**
 * Relay for help to consume transactions
 * on concrete partition from concrete offset
  *
  * @param subscriber Subscriber instance
 * @param partition Partition from which to consume
 * @param coordinator Coordinator instance for maintaining new transactions updates
 * @param callback Callback on consumed transactions
 * @param queue Queue for maintain consumed transactions
 * @tparam DATATYPE Storage data type
 * @tparam USERTYPE User data type
 */
class SubscriberTransactionsRelay[DATATYPE,USERTYPE](subscriber : BasicSubscribingConsumer[DATATYPE,USERTYPE],
                                                     partition : Int,
                                                     coordinator: SubscriberCoordinator,
                                                     callback: BasicSubscriberCallback[DATATYPE, USERTYPE],
                                                     queue : PersistentTransactionQueue,
                                                     lastConsumedTransaction : LastTransactionWrapper,
                                                     executor : ExecutorService,
                                                     checkpointEventsResolver: BrokenTransactionsResolver) {

  private val POOLING_INTERVAL_MS = 100
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val transactionBuffer  = new TransactionsBuffer
  private val transactionBufferLock = new ReentrantLock(true)
  private val streamName = subscriber.stream.getName

  checkpointEventsResolver.bindBuffer(partition, transactionBuffer, transactionBufferLock, lastConsumedTransaction)

  /**
   * Transaction buffer updater
   */
  private val updateCallback = (msg : ProducerTopicMessage) => {
    if (msg.partition == partition) {
      transactionBufferLock.lock()
      logger.debug(s"[UPDATE_CALLBACK PARTITION_$partition] consumed msg with uuid:{${msg.txnUuid.timestamp()}}," +
        s" status:{${msg.status}}\n")
      if (msg.txnUuid.timestamp() > lastConsumedTransaction.get().timestamp()) {
        transactionBuffer.update(msg.txnUuid, msg.status, msg.ttl)
      }
      transactionBufferLock.unlock()

      if (msg.status == ProducerTransactionStatus.preCheckpoint ||
        msg.status == ProducerTransactionStatus.postCheckpoint){
        logger.debug(s"[UPDATE_CALLBACK CER PARTITION_$partition] consumed msg with uuid:{${msg.txnUuid.timestamp()}}," +
          s" status:{${msg.status}}\n")
        checkpointEventsResolver.update(partition, msg.txnUuid, msg.status)
      }
    }
  }

  /**
   * Runnable for consume single txn from persistent queue and callback on it
   */
  private val queueConsumer = new Runnable {
    override def run(): Unit = {
      val txn = queue.get()
      logger.debug(s"[QUEUE_CONSUMER PARTITION_$partition] consumed msg with uuid:{${txn.timestamp()}}")
      callback.onEvent(subscriber, partition, txn)
    }
  }

  /**
   * Consume all transactions in interval (leftBorder ; rightBorder]
   */
  def consumeTransactionsLessOrEqualThan(leftBorder : UUID, rightBorder : UUID) = {
    //TODO remove after complex testing
    var lastTxn : UUID = leftBorder

    val transactionsIterator = subscriber.stream.metadataStorage.commitEntity.getTransactionsIterator(
      streamName = streamName,
      partition = partition,
      leftBorder = leftBorder,
      rightBorder = rightBorder)

    while (transactionsIterator.hasNext) {
      val entry = transactionsIterator.next()
      val (uuid,cnt) = (entry.getUUID("transaction"), entry.getInt("cnt"))
      logger.debug(s"[BEFORE_OR_EQUAL_LAST PARTITION_$partition] consumed txn with uuid:{${uuid.timestamp()}}\n")
      if (cnt == -1) {
        breakable {
          while(true) {
            val updatedTxnOpt = subscriber.
              stream.
              metadataStorage.
              commitEntity.
              getTransactionAmount(streamName, partition, uuid)
            if (updatedTxnOpt.isDefined){
              val (amount,_) = updatedTxnOpt.get
              if (amount != -1){
                queue.put(uuid)
                executor.execute(queueConsumer)
                assert(uuid.timestamp() > lastTxn.timestamp(),
                  logger.debug(s"[RELAY WRONG ASSERT] ${uuid.timestamp()} " +
                    s"with lastTxn={${lastTxn.timestamp()}}\n"))
                lastTxn = uuid
                break()
              }
            }
            else
              break()
            Thread.sleep(POOLING_INTERVAL_MS)
          }
        }
      } else {
        queue.put(uuid)
        executor.execute(queueConsumer)
        assert(uuid.timestamp() > lastTxn.timestamp(),
          logger.debug(s"[RELAY WRONG ASSERT] ${uuid.timestamp()} " +
            s"with lastTxn={${lastTxn.timestamp()}}\n"))
        lastTxn = uuid
      }
    }

    assert(lastTxn.timestamp() == rightBorder.timestamp(),
      logger.debug(s"[RELAY WRONG ASSERT] ${rightBorder.timestamp()} " +
        s"with lastTxn={${lastTxn.timestamp()}}\n"))
  }

  /**
   * Consume all transaction in interval (leftBorder ; inf)
    *
    * @param leftBorder Left interval border
   */
  def consumeTransactionsMoreThan(leftBorder : UUID) = {
    //TODO remove after complex testing
    var lastTxn : UUID = leftBorder
    val transactionsGreaterThanLast =
      subscriber.stream.metadataStorage.commitEntity.getTransactions(
        streamName,
        partition,
        leftBorder)

    transactionBufferLock.lock()
    transactionsGreaterThanLast foreach { txn =>
      logger.debug(s"[MORE_LAST PARTITION_$partition] consumed txn with uuid:{${txn.txnUuid.timestamp()}}\n")
      if (txn.totalItems == -1) {
        transactionBuffer.update(txn.txnUuid, ProducerTransactionStatus.opened, txn.ttl)
      } else {
        transactionBuffer.update(txn.txnUuid, ProducerTransactionStatus.postCheckpoint, -1)
      }
      assert(txn.txnUuid.timestamp() > lastTxn.timestamp(),
        logger.debug(s"[RELAY WRONG ASSERT] ${txn.txnUuid.timestamp()} " +
          s"with lastTxn={${lastTxn.timestamp()}}\n"))
      lastTxn = txn.txnUuid
    }
    transactionBufferLock.unlock()
  }

  /**
   * Notify producers about new subscriber
    *
    * @return Listener ID
   */
  def notifyProducersAndStartListen() : Unit = {
    coordinator.addCallback(updateCallback)
    coordinator.registerSubscriber(subscriber.stream.getName, partition)
    coordinator.notifyProducers(subscriber.stream.getName, partition)
    //wait all producers to connect on this subscriber partition
    coordinator.synchronize(subscriber.stream.getName, partition)
  }

  /**
   * @return Runnable for updating expiring map for this relay
   */
  def getUpdateRunnable() : Runnable = {
    //TODO remove after complex testing
    var totalAmount = 1
    val runnable = new Runnable {
      override def run(): Unit = {
        transactionBufferLock.lock()
        val it = transactionBuffer.getIterator()
        breakable {
          while (it.hasNext) {
            val entry = it.next()
            val key: UUID = entry.getKey
            val (status: ProducerTransactionStatus, _) = entry.getValue
            status match {
              case ProducerTransactionStatus.opened |
                   ProducerTransactionStatus.`update` |
                   ProducerTransactionStatus.preCheckpoint =>
                break()

              case ProducerTransactionStatus.`postCheckpoint` =>
                logger.debug(s"[QUEUE_UPDATER PARTITION_$partition] ${key.timestamp()}" +
                  s" last_consumed=${lastConsumedTransaction.get().timestamp()} curr_amount=$totalAmount\n")
                totalAmount += 1
                queue.put(key)
                executor.execute(queueConsumer)
            }

            //TODO remove after complex testing
            if (lastConsumedTransaction.get().timestamp() >= key.timestamp())
              throw new IllegalStateException("incorrect subscriber state")

            lastConsumedTransaction.set(key)
            it.remove()
          }
        }
        transactionBufferLock.unlock()
      }
    }
    runnable
  }
}
