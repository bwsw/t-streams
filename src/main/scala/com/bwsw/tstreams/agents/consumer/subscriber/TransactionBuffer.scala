package com.bwsw.tstreams.agents.consumer.subscriber


import com.bwsw.tstreamstransactionserver.protocol.TransactionState

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
    * @param update
    */
  def update(update: TransactionState): Unit = this.synchronized {

    val updateNormalized = update.withMasterID(Math.abs(update.masterID))

    // updateState: TransactionState
    //val updateNormalized = updateState.copy()

    updateNormalized.status match {
      case TransactionState.Status.Opened => counters.openEvents.incrementAndGet()
      case TransactionState.Status.Cancelled => counters.cancelEvents.incrementAndGet()
      case TransactionState.Status.Updated => counters.updateEvents.incrementAndGet()
      case TransactionState.Status.Checkpointed => counters.checkpointEvents.incrementAndGet()
      case TransactionState.Status.Instant => counters.instantEvents.incrementAndGet()
      case _ =>
    }

    // avoid transactions which are delayed
    if (updateNormalized.status == TransactionState.Status.Opened) {
      if (lastTransaction != 0L && updateNormalized.transactionID <= lastTransaction) {
        Subscriber.logger.warn(s"Unexpected transaction comparison result ${updateNormalized.transactionID} vs $lastTransaction detected.")
        return
      }
      lastTransaction = updateNormalized.transactionID
    }

    if (stateMap.contains(updateNormalized.transactionID)) {
      val ts = stateMap(updateNormalized.transactionID)
      val orderID = ts.orderID

      stateMap(updateNormalized.transactionID) = ts.withMasterID(updateNormalized.masterID)

      /*
      * state switching system (almost finite automate)
      * */
      (ts.status, updateNormalized.status) match {

        case (TransactionState.Status.Opened, TransactionState.Status.Updated) =>
          stateMap(updateNormalized.transactionID) = ts
            .withOrderID(orderID)
            .withStatus(TransactionState.Status.Opened)
            .withTtlMs(System.currentTimeMillis() + updateNormalized.ttlMs)

        case (TransactionState.Status.Opened, TransactionState.Status.Cancelled) =>
          stateMap(updateNormalized.transactionID) = ts
            .withStatus(TransactionState.Status.Invalid)
            .withTtlMs(0L)
            .withOrderID(orderID)

        case (TransactionState.Status.Opened, TransactionState.Status.Checkpointed) =>
          stateMap(updateNormalized.transactionID) = ts
            .withOrderID(orderID)
            .withStatus(TransactionState.Status.Checkpointed)
            .withCount(updateNormalized.count)
            .withTtlMs(Long.MaxValue)

        case (_, _) =>
          Subscriber.logger.warn(s"Transaction updateNormalized $updateNormalized switched from ${ts.status} to ${updateNormalized.status} which is incorrect. " +
            s"It might be that we cleared StateList because it's size has became greater than ${transactionQueueMaxLengthThreshold}. Try to find clearing notification before.")
      }

    } else {
       updateNormalized.status match {
         case TransactionState.Status.Opened =>
          val upd = updateNormalized.withTtlMs(System.currentTimeMillis() + updateNormalized.ttlMs)
          stateMap(updateNormalized.transactionID) = upd
          stateList.append(upd.transactionID)
         case TransactionState.Status.Instant =>
           val upd = updateNormalized.withStatus(TransactionState.Status.Checkpointed).withTtlMs(Long.MaxValue)
           stateMap(updateNormalized.transactionID) = upd
           stateList.append(upd.transactionID)
         case _ =>
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