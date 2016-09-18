package com.bwsw.tstreams.agents.consumer.subscriber


import com.bwsw.tstreams.common.TransactionComparator
import com.bwsw.tstreams.coordination.messages.state.TransactionStatus

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object TransactionBuffer {
  val MAX_POST_CHECKPOINT_WAIT = 2000

}

/**
  * Created by ivan on 15.09.16.
  */
class TransactionBuffer(queue: QueueBuilder.QueueType) {


  val counters = TransactionBufferCounters()
  var lastTransaction = 0L

  var stateList = ListBuffer[TransactionState]()
  val stateMap = mutable.HashMap[Long, TransactionState]()

  def getQueue(): QueueBuilder.QueueType = queue

  /**
    * Returns current state by transaction ID
    *
    * @param id
    * @return
    */
  def getState(id: Long): Option[TransactionState] = this.synchronized(stateMap.get(id))

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
      case TransactionStatus.preCheckpoint => counters.preCheckpointEvents.incrementAndGet()
      case TransactionStatus.postCheckpoint => counters.postCheckpointEvents.incrementAndGet()
    }

    // avoid transactions which are delayed
    if (update.state == TransactionStatus.opened) {
      if (lastTransaction != 0L
        && TransactionComparator.compare(update.transactionID, lastTransaction) != 1) {
        Subscriber.logger.warn(s"Unexpected transaction comparison result ${update.transactionID} vs $lastTransaction detected.")
        return
      }
      lastTransaction = update.transactionID
    }


    if (stateMap.contains(update.transactionID)) {
      val orderID = stateMap.get(update.transactionID).get.queueOrderID
      val ts = stateMap.get(update.transactionID).get
      /*
      * state switching system (almost finite automate)
      * */
      (ts.state, update.state) match {
        /*
        from opened to *
         */
        case (TransactionStatus.opened, TransactionStatus.opened) =>

        case (TransactionStatus.opened, TransactionStatus.update) =>
          ts.queueOrderID = orderID
          ts.state = TransactionStatus.opened
          ts.ttl = System.currentTimeMillis() + update.ttl * 1000

        case (TransactionStatus.opened, TransactionStatus.preCheckpoint) =>
          ts.queueOrderID = orderID
          ts.state = TransactionStatus.preCheckpoint
          ts.itemCount = update.itemCount
          ts.ttl = System.currentTimeMillis() + TransactionBuffer.MAX_POST_CHECKPOINT_WAIT // TODO: fixit

        case (TransactionStatus.opened, TransactionStatus.postCheckpoint) =>

        case (TransactionStatus.opened, TransactionStatus.cancel) =>
          ts.state = TransactionStatus.invalid
          ts.ttl = 0L
          stateMap.remove(update.transactionID)

        /*
        from update -> * no implement because opened
         */
        case (TransactionStatus.update, _) =>

        /*

           */
        case (TransactionStatus.invalid, _) =>

        /*
        from cancel -> * no implement because removed
        */
        case (TransactionStatus.cancel, _) =>

        /*
        from pre -> *
         */
        case (TransactionStatus.preCheckpoint, TransactionStatus.cancel) =>
          ts.state = TransactionStatus.invalid
          ts.ttl = 0L
          stateMap.remove(update.transactionID)

        case (TransactionStatus.preCheckpoint, TransactionStatus.postCheckpoint) =>
          ts.queueOrderID = orderID
          ts.state = TransactionStatus.postCheckpoint
          ts.itemCount = update.itemCount
          ts.ttl = Long.MaxValue

        case (TransactionStatus.preCheckpoint, _) =>

        /*
        from post -> *
         */
        case (TransactionStatus.postCheckpoint, _) =>
      }

    } else {
      if (update.state == TransactionStatus.opened) {
        update.ttl = System.currentTimeMillis() + update.ttl * 1000
        stateMap.put(update.transactionID, update)
        stateList.append(update)
      }
    }

  }

  def signalCompleteTransactions() = this.synchronized {
    val time = System.currentTimeMillis()

    val meet = stateList.takeWhile(ts =>
      (ts.state == TransactionStatus.postCheckpoint
        || ts.state == TransactionStatus.invalid
        || ts.ttl < time))

    if(meet.nonEmpty) {
      stateList.remove(0, meet.size)

      meet.foreach(ts => stateMap.remove(ts.transactionID))

      queue.put(meet.filter(ts =>
        (ts.state == TransactionStatus.postCheckpoint)
          || (ts.state == TransactionStatus.preCheckpoint && ts.ttl < time)).toList)
    }
  }
}