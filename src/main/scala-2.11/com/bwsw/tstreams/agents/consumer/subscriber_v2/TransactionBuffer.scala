package com.bwsw.tstreams.agents.consumer.subscriber_v2

import java.util.UUID

import com.bwsw.tstreams.common.{SortedExpiringMap, UUIDComparator}
import com.bwsw.tstreams.coordination.messages.state.TransactionStatus

import scala.collection.mutable
import scala.util.control.Breaks._

/**
  * Created by Ivan Kudryavtsev on 19.08.16.
  * Class is used to store states of all transactions at subscribing consumer
  */
class TransactionBuffer(queue: QueueBuilder.QueueType) {

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

    //ignore update events until txn doesn't exist in buffer
    if (!map.exists(update.uuid) && update.state != TransactionStatus.opened) {
      return
    }

    if (map.exists(update.uuid)) {
      map.get(update.uuid).state match {
        case TransactionStatus.preCheckpoint =>
          if (!(update.state == TransactionStatus.postCheckpoint ||
            update.state == TransactionStatus.cancel)) {
            return
          }
        case TransactionStatus.postCheckpoint =>
          return

        case _ =>
      }
    }

    update.state match {
      case TransactionStatus.update | TransactionStatus.opened =>
        map.put(update.uuid, update.copy(state = TransactionStatus.opened))

      //ignore ttl, preCheckpoint will be resolved by another thread
      case TransactionStatus.preCheckpoint =>
        map.put(update.uuid, update.copy(ttl = - 1))

      //just ignore ttl because transaction is closed
      case TransactionStatus.postCheckpoint =>
        map.put(update.uuid, update.copy(ttl = - 1))

      case TransactionStatus.cancel =>
        map.remove(update.uuid)
    }
  }

  /**
    * This method throws ready sequences of [[TransactionState]] to [[com.bwsw.tstreams.common.AbstractQueue]]
    */
  def signalCompleteTransactions() = this.synchronized {
    val it = map.entrySetIterator()
    breakable {
      val completeList = mutable.ListBuffer[TransactionState]()
      while (it.hasNext) {
        val entry = it.next()
        entry.getValue.state match {
          case TransactionStatus.postCheckpoint =>
            completeList.append(entry.getValue)
          case _ =>
            if(completeList.size > 0) {
              queue.put(completeList.toList)
              completeList foreach (i => map.remove(i.uuid))
            }
            break()
        }
      }
      if(completeList.size > 0) {
        queue.put(completeList.toList)
        completeList foreach (i => map.remove(i.uuid))
      }
    }
  }
}
