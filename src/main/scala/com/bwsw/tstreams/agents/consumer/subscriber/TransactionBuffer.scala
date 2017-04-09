package com.bwsw.tstreams.agents.consumer.subscriber


import com.bwsw.tstreams.coordination.messages.state.TransactionStatus

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object TransactionBuffer {
  val MAX_POST_CHECKPOINT_WAIT = 2000

}

/**
  * Created by ivan on 15.09.16.
  */
class TransactionBuffer(queue: QueueBuilder.QueueType, transactionQueueMaxLengthThreshold: Int = 1000) {


  val counters = TransactionBufferCounters()
  var lastTransaction = 0L

  private val stateList = ListBuffer[TransactionState]()
  private val stateMap = mutable.HashMap[Long, TransactionState]()

  def getQueue(): QueueBuilder.QueueType = queue

  /**
    * Returns current state by transaction ID
    *
    * @param id
    * @return
    */
  def getState(id: Long): Option[TransactionState] = this.synchronized(stateMap.get(id))

  /**
    * returns size of the list
    *
    * @return
    */
  def getSize(): Int = stateList.size

  /**
    * This method updates buffer with new state
    *
    * @param updateState
    */
  def update(updateState: TransactionState): Unit = this.synchronized {

    val update = updateState.copy()

    update.state match {
      case TransactionStatus.opened => counters.openEvents.incrementAndGet()
      case TransactionStatus.cancel => counters.cancelEvents.incrementAndGet()
      case TransactionStatus.update => counters.updateEvents.incrementAndGet()
      case TransactionStatus.`checkpointed` => counters.checkpointEvents.incrementAndGet()
    }

    // avoid transactions which are delayed
    if (update.state == TransactionStatus.opened) {
      if (lastTransaction != 0L
        && update.transactionID <= lastTransaction) {
        Subscriber.logger.warn(s"Unexpected transaction comparison result ${update.transactionID} vs $lastTransaction detected.")
        return
      }
      lastTransaction = update.transactionID
    }


    if (stateMap.contains(update.transactionID)) {
      val ts = stateMap(update.transactionID)
      val orderID = ts.queueOrderID

      // If master is changed and we the event has been received via another master then it's bad case.
      // Set new master to avoid fast loading (additional protection is done through orderID.
      //
      ts.masterSessionID = update.masterSessionID

      /*
      * state switching system (almost finite automate)
      * */
      (ts.state, update.state) match {

        case (TransactionStatus.opened, TransactionStatus.update) =>
          ts.queueOrderID = orderID
          ts.state = TransactionStatus.opened
          ts.ttlMs = System.currentTimeMillis() + update.ttlMs

        case (TransactionStatus.opened, TransactionStatus.cancel) =>
          ts.state = TransactionStatus.invalid
          ts.ttlMs = 0L
          stateMap.remove(update.transactionID)

        case (TransactionStatus.opened, TransactionStatus.`checkpointed`) =>
          ts.queueOrderID = orderID
          ts.state = TransactionStatus.checkpointed
          ts.itemCount = update.itemCount
          ts.ttlMs = Long.MaxValue

        case (_, _) =>
          Subscriber.logger.warn(s"Transaction update $update switched from ${ts.state} to ${update.state} which is incorrect. " +
            "It might be that we cleared StateList because it's size has became greater than ${subscriberOptions.transactionQueueMaxLengthThreshold}. Try to find clearing notification before.")
      }

    } else {
      if (update.state == TransactionStatus.opened) {
        update.ttlMs = System.currentTimeMillis() + update.ttlMs
        stateMap.put(update.transactionID, update)
        stateList.append(update)
      }
    }

  }

  def signalCompleteTransactions(): Unit = this.synchronized {
    val time = System.currentTimeMillis()

    val meetCheckpoint = stateList.takeWhile(ts => ts.state == TransactionStatus.checkpointed)

    meetCheckpoint.foreach(ts => stateMap.remove(ts.transactionID))

    if (meetCheckpoint.nonEmpty) {
      stateList.remove(0, meetCheckpoint.size)
      queue.put(meetCheckpoint.toList)
    }

    if (transactionQueueMaxLengthThreshold <= stateList.size) {
      Subscriber.logger.warn(s"Transaction StateList achieved ${stateList.size} items. The threshold is $transactionQueueMaxLengthThreshold items. Clear it to protect the memory. " +
        "It seems that the user part handles transactions slower than producers feed me.")
      stateList.clear()
      stateMap.clear()
      return
    }

    val meetTimeoutAndInvalid = stateList.takeWhile(ts => ts.ttlMs < time)

    if (meetTimeoutAndInvalid.nonEmpty) {
      meetTimeoutAndInvalid.foreach(ts => stateMap.remove(ts.transactionID))
      stateList.remove(0, meetTimeoutAndInvalid.size)
    }

  }
}