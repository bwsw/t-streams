package com.bwsw.tstreams.agents.consumer.subscriber_v2

import java.util.UUID

import com.bwsw.tstreams.common.{SortedExpiringMap, UUIDComparator}
import com.bwsw.tstreams.coordination.messages.state.TransactionStatus

import scala.collection.mutable

/**
  * Created by Ivan Kudryavtsev on 19.08.16.
  * Class is used to store states of all transactions at subscribing consumer
  */
class TransactionBuffer(queue: QueueBuilder.QueueType) {

  var lastTransaction: UUID = null
  val comparator = new UUIDComparator()

  def getQueue(): QueueBuilder.QueueType = queue

  private val map: SortedExpiringMap[UUID, TransactionState] =
    new SortedExpiringMap(new UUIDComparator, new TransactionStateExpirationPolicy)

  /**
    * Returnes current state by transaction uuid
    * @param uuid
    * @return
    */
  def getState(uuid: UUID): Option[TransactionState] =
    if (map.exists(uuid)) Option(map.get(uuid)) else None

  /**
    * This method updates buffer with new state
    * @param update
    */
  def update(update: TransactionState): Unit = this.synchronized {

    // avoid transactions which are delayed
    if (update.state == TransactionStatus.opened) {
      if(lastTransaction != null
        && comparator.compare(update.uuid, lastTransaction) != 1)
        return
    }

    if (map.exists(update.uuid)) {

      /*
      * finite automata
      * */
      (map.get(update.uuid).state, update.state) match {
          /*
          from opened to *
           */
        case (TransactionStatus.opened, TransactionStatus.opened) =>

        case (TransactionStatus.opened, TransactionStatus.update) =>
          map.put(update.uuid, update.copy(state = TransactionStatus.opened))

        case (TransactionStatus.opened, TransactionStatus.preCheckpoint) =>
          map.put(update.uuid, update.copy(ttl = - 1))

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
          map.put(update.uuid, update.copy(ttl = - 1))

        case (TransactionStatus.preCheckpoint, _) =>

          /*
          from post -> *
           */
        case (TransactionStatus.postCheckpoint, _) =>
      }

    } else {
      if (update.state == TransactionStatus.opened) {
        map.put(update.uuid, update)
        lastTransaction = update.uuid
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
