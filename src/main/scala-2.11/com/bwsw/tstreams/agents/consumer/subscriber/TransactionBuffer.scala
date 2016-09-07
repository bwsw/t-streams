package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import com.bwsw.tstreams.common.{SortedExpiringMap, UUIDComparator}
import com.bwsw.tstreams.coordination.messages.state.TransactionStatus

import scala.collection.mutable

case class TransactionBufferCounters(openEvents: AtomicLong,
                                     cancelEvents: AtomicLong,
                                     updateEvents: AtomicLong,
                                     preCheckpointEvents: AtomicLong,
                                     postCheckpointEvents: AtomicLong) {
  def dump(partition: Int): Unit = {
    Subscriber.logger.info(s"Partitions ${partition} - Open Events received: ${openEvents.get()}")
    Subscriber.logger.info(s"Partitions ${partition} - Cancel Events received: ${cancelEvents.get()}")
    Subscriber.logger.info(s"Partitions ${partition} - Update Events received: ${updateEvents.get()}")
    Subscriber.logger.info(s"Partitions ${partition} - PreCheckpoint Events received: ${preCheckpointEvents.get()}")
    Subscriber.logger.info(s"Partitions ${partition} - PostCheckpoint Events received: ${postCheckpointEvents.get()}")
  }
}

/**
  * Created by Ivan Kudryavtsev on 19.08.16.
  * Class is used to store states of all transactions at subscribing consumer
  */
class TransactionBuffer(queue: QueueBuilder.QueueType) {


  val counters = TransactionBufferCounters(new AtomicLong(0),
    new AtomicLong(0),
    new AtomicLong(0),
    new AtomicLong(0),
    new AtomicLong(0))

  var lastTransaction: UUID = null
  val comparator = new UUIDComparator()

  def getQueue(): QueueBuilder.QueueType = queue

  private val map: SortedExpiringMap[UUID, TransactionState] =
    new SortedExpiringMap(new UUIDComparator, new TransactionStateExpirationPolicy)

  /**
    * Returnes current state by transaction uuid
    *
    * @param uuid
    * @return
    */
  def getState(uuid: UUID): Option[TransactionState] =
    if (map.exists(uuid)) Option(map.get(uuid)) else None

  /**
    * This method updates buffer with new state
    *
    * @param update
    */
  def update(update: TransactionState): Unit = this.synchronized {

    update.state match {
      case TransactionStatus.opened => counters.openEvents.incrementAndGet()
      case TransactionStatus.cancel => counters.cancelEvents.incrementAndGet()
      case TransactionStatus.update => counters.updateEvents.incrementAndGet()
      case TransactionStatus.preCheckpoint => counters.preCheckpointEvents.incrementAndGet()
      case TransactionStatus.postCheckpoint => counters.postCheckpointEvents.incrementAndGet()
    }

    // avoid transactions which are delayed
    if (update.state == TransactionStatus.opened) {
      if(lastTransaction != null
        && comparator.compare(update.uuid, lastTransaction) != 1) {
        Subscriber.logger.warn(s"Unexpected transaction comparison result ${update.uuid} vs ${lastTransaction} detected.")
        return
      }
      lastTransaction = update.uuid
    }


    if (map.exists(update.uuid)) {
      val orderID = map.get(update.uuid).queueOrderID
      /*
      * state switching system (almost finite automate)
      * */
      (map.get(update.uuid).state, update.state) match {
          /*
          from opened to *
           */
        case (TransactionStatus.opened, TransactionStatus.opened) =>

        case (TransactionStatus.opened, TransactionStatus.update) =>
          map.put(update.uuid, update.copy(state = TransactionStatus.opened, queueOrderID = orderID))

        case (TransactionStatus.opened, TransactionStatus.preCheckpoint) =>
          map.put(update.uuid, update.copy(ttl = - 1, queueOrderID = orderID))

        case (TransactionStatus.opened, TransactionStatus.postCheckpoint) =>

        case (TransactionStatus.opened, TransactionStatus.cancel) =>
          map.remove(update.uuid)

          /*
          from update -> * no implement because opened
           */
        case (TransactionStatus.update, _) =>

          /*
          from cancel -> * no implement because removed
          */
        case (TransactionStatus.cancel, _) =>

          /*
          from pre -> *
           */
        case (TransactionStatus.preCheckpoint, TransactionStatus.cancel) =>
          map.remove(update.uuid)

        case (TransactionStatus.preCheckpoint, TransactionStatus.postCheckpoint) =>
          map.put(update.uuid, update.copy(ttl = - 1, queueOrderID = orderID))

        case (TransactionStatus.preCheckpoint, _) =>

          /*
          from post -> *
           */
        case (TransactionStatus.postCheckpoint, _) =>
      }

    } else {
      if (update.state == TransactionStatus.opened) {
        map.put(update.uuid, update)
      }
    }

  }

  /**
    * This method throws ready sequences of [[TransactionState]] to [[com.bwsw.tstreams.common.AbstractQueue]]
    */
  def signalCompleteTransactions() = this.synchronized {
    val it = map.entrySetIterator()
    val completeList = mutable.ListBuffer[TransactionState]()
    var continue = true
    while (it.hasNext && continue) {
      val entry = it.next()
      entry.getValue.state match {
        case TransactionStatus.postCheckpoint => completeList.append(entry.getValue)
        case _ => continue = false
      }
    }
    if(completeList.size > 0) {
      queue.put(completeList.toList)
      completeList foreach (i => map.remove(i.uuid))
    }
  }
}
