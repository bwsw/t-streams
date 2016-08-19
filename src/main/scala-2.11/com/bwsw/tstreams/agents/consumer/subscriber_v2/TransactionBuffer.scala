package com.bwsw.tstreams.agents.consumer.subscriber_v2

import java.util.UUID
import com.bwsw.tstreams.common.{UUIDComparator, SortedExpiringMap}
import com.bwsw.tstreams.coordination.messages.state.TransactionStatus

/**
  * Created by ivan on 19.08.16.
  */
class TransactionBuffer(partition: Int,
                        queue: QueueBuilder.QueueType) {

  private val map: SortedExpiringMap[UUID, TransactionState] =
    new SortedExpiringMap(new UUIDComparator, new TransactionStateExpirationPolicy)

  def signal(update: TransactionState): Unit = {

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
        map.put(update.uuid, update)

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

  def signalCompleteTransactions() = {

  }
}
