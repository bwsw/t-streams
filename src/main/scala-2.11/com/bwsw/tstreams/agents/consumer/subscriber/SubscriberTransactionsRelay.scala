package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.UUID
import java.util.concurrent.ExecutorService
import java.util.concurrent.locks.ReentrantLock
import com.bwsw.tstreams.coordination.pubsub.ConsumerCoordinator
import com.bwsw.tstreams.coordination.pubsub.messages.{ProducerTopicMessage, ProducerTransactionStatus}
import ProducerTransactionStatus._
import com.bwsw.tstreams.txnqueue.PersistentTransactionQueue
import org.slf4j.LoggerFactory
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._


/**
 * Class for consuming transactions on concrete partition from concrete offset
 * @param subscriber Subscriber instance which instantiate this relay
 * @param offset Offset from which to start
 * @param partition Partition from which to consume
 * @param coordinator Coordinator instance for maintaining new transactions updates
 * @param callback Callback on consumed transactions
 * @param queue Queue for maintain consumed transactions
 * @tparam DATATYPE Storage data type
 * @tparam USERTYPE User data type
 */
class SubscriberTransactionsRelay[DATATYPE,USERTYPE](subscriber : BasicSubscribingConsumer[DATATYPE,USERTYPE],
                                                     offset: UUID,
                                                     partition : Int,
                                                     coordinator: ConsumerCoordinator,
                                                     callback: BasicSubscriberCallback[DATATYPE, USERTYPE],
                                                     queue : PersistentTransactionQueue,
                                                     lastTransaction : UUID,
                                                     executor : ExecutorService) {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val transactionBuffer  = new TransactionsBuffer
  private val lock = new ReentrantLock(true)
  private var lastConsumedTransaction : UUID = lastTransaction
  private val streamName = subscriber.stream.getName

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
   * Transaction buffer updater
   */
  private val updateCallback = (msg : ProducerTopicMessage) => {
    if (msg.partition == partition) {
      lock.lock()
      logger.debug(s"[UPDATE_CALLBACK PARTITION_$partition] consumed msg with uuid:{${msg.txnUuid.timestamp()}}," +
        s" status:{${msg.status}}\n")
      if (msg.txnUuid.timestamp() > lastConsumedTransaction.timestamp()) {
        transactionBuffer.update(msg.txnUuid, msg.status, msg.ttl)
      }
      lock.unlock()
    }
  }

  /**
   * Consume all transactions in interval (offset ; transactionUUID]
   * @param transactionUUID Right border to consume
   */
  def consumeTransactionsLessOrEqualThan(transactionUUID : UUID) = {
    //TODO remove after debug
    var lastTxn : UUID = subscriber.options.txnGenerator.getTimeUUID(0)
    val runnable = new Runnable {
      override def run(): Unit = {
        val transactionsIterator = subscriber.stream.metadataStorage.commitEntity.getTransactionsIterator(
          streamName = streamName,
          partition = partition,
          leftBorder = offset,
          rightBorder = transactionUUID)

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
                Thread.sleep(100)
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

        assert(lastTxn.timestamp() == transactionUUID.timestamp(),
          logger.debug(s"[RELAY WRONG ASSERT] ${transactionUUID.timestamp()} " +
            s"with lastTxn={${lastTxn.timestamp()}}\n"))
      }
    }

    executor.execute(runnable)
  }

  /**
   * Consume all transaction in interval (transactionUUID ; inf)
   * @param transactionUUID Left border to consume
   */
  def consumeTransactionsMoreThan(transactionUUID : UUID) = {
    //TODO remove after debug
    var lastTxn : UUID = lastTransaction
    val transactionsGreaterThanLast =
      subscriber.stream.metadataStorage.commitEntity.getTransactions(
        streamName,
        partition,
        transactionUUID)

    lock.lock()
    transactionsGreaterThanLast foreach { txn =>
      logger.debug(s"[MORE_LAST PARTITION_$partition] consumed txn with uuid:{${txn.txnUuid.timestamp()}}\n")
      if (txn.totalItems == -1) {
        transactionBuffer.update(txn.txnUuid, ProducerTransactionStatus.opened, txn.ttl)
      } else {
        transactionBuffer.update(txn.txnUuid, ProducerTransactionStatus.closed, -1)
      }
      assert(txn.txnUuid.timestamp() > lastTxn.timestamp(),
        logger.debug(s"[RELAY WRONG ASSERT] ${txn.txnUuid.timestamp()} " +
          s"with lastTxn={${lastTxn.timestamp()}}\n"))
      lastTxn = txn.txnUuid
    }
    lock.unlock()
  }

  /**
   * Notify producers about new subscriber
   * @return Listener ID
   */
  def notifyProducersAndStartListen() : Unit = {
    coordinator.addCallback(updateCallback)
    coordinator.registerSubscriber(subscriber.stream.getName, partition)
    coordinator.notifyProducers(subscriber.stream.getName, partition)
    coordinator.synchronize(subscriber.stream.getName, partition)
  }

  /**
   * Runnable for updating expiring map
   */
  def getUpdateRunnable() : Runnable = {
    var totalAmount = 1 //TODO for logging
    val runnable = new Runnable {
      override def run(): Unit = {
        lock.lock()
        logTransactionBuffer() //TODO remove after hard debug
        val it = transactionBuffer.getIterator()
        breakable {
          while (it.hasNext) {
            val entry = it.next()
            val key: UUID = entry.getKey
            val (status: ProducerTransactionStatus, _) = entry.getValue
            status match {
              case ProducerTransactionStatus.opened =>
                break()

              case ProducerTransactionStatus.updated =>
                break()

              case ProducerTransactionStatus.cancelled =>
                throw new IllegalStateException

              case ProducerTransactionStatus.closed =>
                logger.debug(s"[QUEUE_UPDATER PARTITION_$partition] ${key.timestamp()}" +
                  s" last_consumed=${lastConsumedTransaction.timestamp()} curr_amount=$totalAmount\n")
                totalAmount += 1
                queue.put(key)
                executor.execute(queueConsumer)
            }

            //TODO remove after complex testing
            if (lastConsumedTransaction.timestamp() >= key.timestamp())
              throw new IllegalStateException("incorrect subscriber state")

            lastConsumedTransaction = key
            it.remove()
          }
        }
        lock.unlock()
      }
    }
    runnable
  }

  //TODO remove after hard debug
  def logTransactionBuffer() = {
    val lb = ListBuffer[(Long, ProducerTransactionStatus)]()
    val it = transactionBuffer.getIterator()
    while (it.hasNext){
      val entry = it.next()
      lb += ((entry.getKey.timestamp(), entry.getValue._1))
    }
    logger.debug(s"[QUEUE_UPDATER PARTITION_$partition] ${lb.toList}\n")
  }
}
