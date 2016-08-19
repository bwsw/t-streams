package com.bwsw.tstreams.agents.consumer.subscriber_v2

import java.util.UUID
import com.bwsw.tstreams.common.{UUIDComparator, SortedExpiringMap}
import com.bwsw.tstreams.coordination.messages.state.TransactionStatus

import scala.collection.mutable
import scala.util.control.Breaks._

/**
  * Created by ivan on 19.08.16.
  */
class TransactionBuffer(queue: QueueBuilder.QueueType) {

  private val map: SortedExpiringMap[UUID, TransactionState] =
    new SortedExpiringMap(new UUIDComparator, new TransactionStateExpirationPolicy)

  def getState(uuid: UUID): Option[TransactionState] =
    if (map.exists(uuid)) Option(map.get(uuid)) else None

  def update(update: TransactionState): Unit = this.synchronized {

    //ignore update events until txn doesn't exist in buffer
    if (!map.exists(update.uuid) && update.state == TransactionStatus.update) {
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
      case TransactionStatus.`postCheckpoint` =>
        map.put(update.uuid, update.copy(ttl = - 1))

      case TransactionStatus.`cancel` =>
        map.remove(update.uuid)
    }
  }

  def signalCompleteTransactions() = this.synchronized {
    val it = map.entrySetIterator()
    breakable {
      val completeList = mutable.ListBuffer[TransactionState]()
      while (it.hasNext) {
        val entry = it.next()
        val key: UUID = entry.getKey
        entry.getValue.state match {
          case TransactionStatus.postCheckpoint =>
            completeList.append(entry.getValue)
          case _ => break()
        }
      }
      queue.put(completeList.toList)
    }
  }
}
