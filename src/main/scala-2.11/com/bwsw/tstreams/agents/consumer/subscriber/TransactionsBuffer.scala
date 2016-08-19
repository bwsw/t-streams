package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.UUID

import com.bwsw.tstreams.common.{UUIDComparator, SortedExpiringMap}
import com.bwsw.tstreams.coordination.messages.state.TransactionStatus._
import com.bwsw.tstreams.coordination.messages.state.TransactionStatus

/**
  * Buffer for maintaining consumed transactions in memory
  */
class TransactionsBuffer {
  private val map: SortedExpiringMap[UUID, (ProducerTransactionStatus, Long)] =
    new SortedExpiringMap(new UUIDComparator, new SubscriberExpirationPolicy)

  /**
    * Update transaction buffer
    *
    * @param txnUuid Transaction uuid
    * @param status  Transaction status
    * @param ttl     Transaction ttl(time of expiration)
    */
  def update(txnUuid: UUID, status: ProducerTransactionStatus, ttl: Int): Unit = {

    //TODO wrap all checks in validate method and log it
    //ignore update events until txn doesn't exist in buffer
    if (!map.exist(txnUuid) && status == TransactionStatus.update) {
      return
    }

    if (map.exist(txnUuid)) {
      map.get(txnUuid)._1 match {
        case TransactionStatus.preCheckpoint =>
          if (status != TransactionStatus.postCheckpoint &&
            status != TransactionStatus.cancel) {
            return
          }

        case TransactionStatus.`postCheckpoint` =>
          return

        case _ =>
      }
    }

    status match {
      case TransactionStatus.`update` |
           TransactionStatus.opened =>
        map.put(txnUuid, (status, ttl))

      //ignore ttl, preCheckpoint will be resolved by another thread
      case TransactionStatus.preCheckpoint =>
        map.put(txnUuid, (status, -1))

      //just ignore ttl because transaction is closed
      case TransactionStatus.`postCheckpoint` =>
        map.put(txnUuid, (status, -1))

      case TransactionStatus.`cancel` =>
        map.remove(txnUuid)
    }
  }

  /**
    * Iterator on transaction buffer entry set
    *
    * @return Iterator
    */
  def getIterator() = map.entrySetIterator()
}
