package com.bwsw.tstreams.agents.consumer.subscriber


import com.bwsw.tstreams.proto.protocol.TransactionState

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

  private val stateList = ListBuffer[Long]()
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

    update.status match {
      case TransactionState.Status.Opened => counters.openEvents.incrementAndGet()
      case TransactionState.Status.Cancelled => counters.cancelEvents.incrementAndGet()
      case TransactionState.Status.Updated => counters.updateEvents.incrementAndGet()
      case TransactionState.Status.Checkpointed => counters.checkpointEvents.incrementAndGet()
    }

    // avoid transactions which are delayed
    if (update.status == TransactionState.Status.Opened) {
      if (lastTransaction != 0L
        && update.transactionID <= lastTransaction) {
        Subscriber.logger.warn(s"Unexpected transaction comparison result ${update.transactionID} vs $lastTransaction detected.")
        return
      }
      lastTransaction = update.transactionID
    }


    if (stateMap.contains(update.transactionID)) {
      val ts = stateMap(update.transactionID)
      val orderID = ts.orderID

      // If master is changed and we the event has been received via another master then it's bad case.
      // Set new master to avoid fast loading (additional protection is done through orderID.
      //
      //ts.masterID = update.masterID
      stateMap(update.transactionID) = ts.withMasterID(update.masterID)

      /*
      * state switching system (almost finite automate)
      * */
      (ts.status, update.status) match {

        case (TransactionState.Status.Opened, TransactionState.Status.Updated) =>
          stateMap(update.transactionID) = ts
            .withOrderID(orderID)
            .withStatus(TransactionState.Status.Opened)
            .withTtlMs(System.currentTimeMillis() + update.ttlMs)
//            ts.orderID = orderID
//          ts.status = TransactionState.Status.Opened
//          ts.ttlMs = System.currentTimeMillis() + update.ttlMs

        case (TransactionState.Status.Opened, TransactionState.Status.Cancelled) =>
          stateMap(update.transactionID) = ts
            .withStatus(TransactionState.Status.Invalid)
            .withTtlMs(0L)
            .withOrderID(orderID)
//          ts.status = TransactionState.Status.Invalid
//          ts.ttlMs = 0L
//          ts.orderID = orderID
        // added
        //stateMap.remove(update.transactionID)

        case (TransactionState.Status.Opened, TransactionState.Status.Checkpointed) =>
          stateMap(update.transactionID) = ts
            .withOrderID(orderID)
            .withStatus(TransactionState.Status.Checkpointed)
            .withCount(update.count)
            .withTtlMs(Long.MaxValue)

//          ts.orderID = orderID
//          ts.status = TransactionState.Status.Checkpointed
//          ts.count = update.count
//          ts.ttlMs = Long.MaxValue

        case (_, _) =>
          Subscriber.logger.warn(s"Transaction update $update switched from ${ts.status} to ${update.status} which is incorrect. " +
            "It might be that we cleared StateList because it's size has became greater than ${subscriberOptions.transactionQueueMaxLengthThreshold}. Try to find clearing notification before.")
      }

    } else {
      if (update.status == TransactionState.Status.Opened) {
        val upd = update.withTtlMs(System.currentTimeMillis() + update.ttlMs)
        stateMap(update.transactionID) = upd
        stateList.append(upd.transactionID)
      }
    }

  }

  def signalCompleteTransactions(): Unit = this.synchronized {
    val time = System.currentTimeMillis()

    val meetCheckpoint = stateList.takeWhile(ts => {
      val s = stateMap(ts)
      (s.status == TransactionState.Status.Checkpointed || s.status == TransactionState.Status.Invalid)
    })


    if (meetCheckpoint.nonEmpty) {
      stateList.remove(0, meetCheckpoint.size)
      queue.put(meetCheckpoint.map(transaction => stateMap(transaction)).toList)
    }

    meetCheckpoint.foreach(ts => stateMap.remove(ts))

    if (transactionQueueMaxLengthThreshold <= stateList.size) {
      Subscriber.logger.warn(s"Transaction StateList achieved ${stateList.size} items. The threshold is $transactionQueueMaxLengthThreshold items. Clear it to protect the memory. " +
        "It seems that the user part handles transactions slower than producers feed me.")
      stateList.clear()
      stateMap.clear()

      return
    }

    val meetTimeoutAndInvalid = stateList.takeWhile(ts => stateMap(ts).ttlMs < time)

    if (meetTimeoutAndInvalid.nonEmpty) {
      stateList.remove(0, meetTimeoutAndInvalid.size)
      meetTimeoutAndInvalid.foreach(ts => stateMap.remove(ts))
    }

  }
}