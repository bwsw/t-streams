package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.UUID

import com.bwsw.tstreams.coordination.pubsub.messages.ProducerTransactionStatus
import com.bwsw.tstreams.coordination.pubsub.messages.ProducerTransactionStatus._

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
    if (!map.exist(txnUuid) && status == ProducerTransactionStatus.update) {
      return
    }

    if (map.exist(txnUuid)) {
      map.get(txnUuid)._1 match {
        case ProducerTransactionStatus.preCheckpoint =>
          if (status != ProducerTransactionStatus.postCheckpoint &&
            status != ProducerTransactionStatus.cancel) {
            return
          }

        case ProducerTransactionStatus.`postCheckpoint` =>
          return

        case _ =>
      }
    }

    status match {
      case ProducerTransactionStatus.`update` |
           ProducerTransactionStatus.opened =>
        map.put(txnUuid, (status, ttl))

      //ignore ttl, preCheckpoint will be resolved by another thread
      case ProducerTransactionStatus.preCheckpoint =>
        map.put(txnUuid, (status, -1))

      //just ignore ttl because transaction is closed
      case ProducerTransactionStatus.`postCheckpoint` =>
        map.put(txnUuid, (status, -1))

      case ProducerTransactionStatus.`cancel` =>
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
