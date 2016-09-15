package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.UUID

import com.bwsw.tstreams.common.UUIDComparator
import com.bwsw.tstreams.coordination.messages.state.TransactionStatus

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by ivan on 15.09.16.
  */
class TransactionBuffer(queue: QueueBuilder.QueueType) {

  val MAX_POSTCHECKPOINT_WAIT = 2000

  val counters = TransactionBufferCounters()
  var lastTransaction: UUID = null
  val comparator = new UUIDComparator()

  var stateList = ListBuffer[TransactionState]()
  val stateMap = mutable.Map[UUID, TransactionState]()

  def getQueue(): QueueBuilder.QueueType = queue

  /**
    * Returns current state by transaction uuid
    *
    * @param uuid
    * @return
    */
  def getState(uuid: UUID): Option[TransactionState] = this.synchronized(stateMap.get(uuid))

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
      if (lastTransaction != null
        && comparator.compare(update.uuid, lastTransaction) != 1) {
        Subscriber.logger.warn(s"Unexpected transaction comparison result ${update.uuid} vs $lastTransaction detected.")
        return
      }
      lastTransaction = update.uuid
    }


    if (stateMap.contains(update.uuid)) {
      val orderID = stateMap.get(update.uuid).get.queueOrderID
      val ts = stateMap.get(update.uuid).get
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
          ts.ttl = System.currentTimeMillis() + MAX_POSTCHECKPOINT_WAIT // TODO: fixit

        case (TransactionStatus.opened, TransactionStatus.postCheckpoint) =>

        case (TransactionStatus.opened, TransactionStatus.cancel) =>
          ts.state = TransactionStatus.invalid
          ts.ttl = 0L
          stateMap.remove(update.uuid)

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
          stateMap.remove(update.uuid)

        case (TransactionStatus.preCheckpoint, TransactionStatus.postCheckpoint) =>
          ts.queueOrderID = orderID
          ts.state = TransactionStatus.postCheckpoint
          ts.ttl = Long.MaxValue
          //stateMap.remove(update.uuid)

        case (TransactionStatus.preCheckpoint, _) =>

        /*
        from post -> *
         */
        case (TransactionStatus.postCheckpoint, _) =>
      }

    } else {
      if (update.state == TransactionStatus.opened) {
        stateMap.put(update.uuid, update)
        update.ttl = System.currentTimeMillis() + update.ttl * 1000
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

      meet.foreach(ts => stateMap.remove(ts.uuid))

      queue.put(meet.filter(ts =>
        (ts.state == TransactionStatus.postCheckpoint)
          || (ts.state == TransactionStatus.preCheckpoint && ts.ttl < time)).toList)
    }
  }
}