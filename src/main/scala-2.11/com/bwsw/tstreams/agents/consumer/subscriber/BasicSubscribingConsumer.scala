package com.bwsw.tstreams.agents.consumer.subscriber

import com.bwsw.tstreams.agents.consumer.{BasicConsumer, BasicConsumerOptions}
import com.bwsw.tstreams.streams.BasicStream
import com.bwsw.tstreams.txnqueue.PersistentTransactionQueue

import scala.collection.mutable.ListBuffer

/**
 * Basic consumer with subscribe option
 * @param name Name of consumer
 * @param stream Stream from which to consume transactions
 * @param options Basic consumer options
 * @param persistentQueuePath Local Path to queue which maintain transactions that already exist and new incoming transactions
 * @tparam DATATYPE Storage data type
 * @tparam USERTYPE User data type
 */
class BasicSubscribingConsumer[DATATYPE, USERTYPE](name : String,
                                                   stream : BasicStream[DATATYPE],
                                                   options : BasicConsumerOptions[DATATYPE,USERTYPE],
                                                   callBack : BasicSubscriberCallback[DATATYPE, USERTYPE],
                                                   persistentQueuePath : String)
  extends BasicConsumer[DATATYPE, USERTYPE](name, stream, options){

  /**
   * Current subscriber state
   */
  private var isStarted = false


  private var relays = ListBuffer[SubscriberTransactionsRelay[_,_]]()

  /**
   * Start to consume messages
   */
  def start() = {
    if (isStarted)
      throw new IllegalStateException("subscriber already started")
    isStarted = true

    coordinator.startListen()

    //TODO [THINK HERE] mb move it above [split callback on partitions] MAP[partition, queue[txn]]
    coordinator.startCallback()

    (0 until stream.getPartitions) foreach { partition =>
      //getting last txn for concrete partition
      val lastTransactionOpt = getLastTransaction(partition)

      //creating queue for concrete partition
      val queue =
        if (lastTransactionOpt.isDefined) {
          val txnUuid = lastTransactionOpt.get.getTxnUUID
          new PersistentTransactionQueue(persistentQueuePath + s"/$partition", txnUuid)
        }
        else {
          new PersistentTransactionQueue(persistentQueuePath + s"/$partition", null)
        }

      val transactionsRelay = new SubscriberTransactionsRelay(subscriber = this,
        offset = currentOffsets(partition),
        partition = partition,
        coordinator = coordinator,
        callback = callBack,
        queue = queue)

      relays += transactionsRelay

      //start tread to consume queue and doing callback's on it
      transactionsRelay.startConsumeAndCallbackQueueAsync()

      //start tread to consume all transactions before lasttxn including it
      if (lastTransactionOpt.isDefined)
        transactionsRelay.consumeTransactionsLessOrEqualThanAsync(lastTransactionOpt.get.getTxnUUID)

      transactionsRelay.notifyProducers()

      //consume all messages greater than last
      if (lastTransactionOpt.isDefined)
        transactionsRelay.consumeTransactionsMoreThan(lastTransactionOpt.get.getTxnUUID)
      else {
        val oldestUuid = options.txnGenerator.getTimeUUID(0)
        transactionsRelay.consumeTransactionsMoreThan(oldestUuid)
      }

      transactionsRelay.startUpdate()
    }

    coordinator.synchronize(stream.getName, (0 until stream.getPartitions).toList)
  }

  /**
   * Stop consumer handle incoming messages
   */
  override def stop() = {
    if (!isStarted)
      throw new IllegalStateException("subscriber is not started")
    isStarted = false
    relays.foreach(_.stop())
    relays.clear()
    coordinator.stop()
  }
}
