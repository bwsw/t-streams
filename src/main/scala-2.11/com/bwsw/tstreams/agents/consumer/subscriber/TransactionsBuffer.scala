package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.UUID
import com.bwsw.tstreams.coordination.pubsub.messages.ProducerTransactionStatus
import ProducerTransactionStatus._
import com.bwsw.tstreams.coordination.pubsub.messages.ProducerTransactionStatus

/**
 * Buffer for maintaining consumed transactions in memory
 */
class TransactionsBuffer {
  private val map : SortedExpiringMap[UUID, (ProducerTransactionStatus, Long)] =
    new SortedExpiringMap(new UUIDComparator, new SubscriberExpirationPolicy)

  /**
   * Update transaction buffer
   * @param txnUuid Transaction uuid
   * @param status Transaction status
   * @param ttl Transaction ttl(time of expiration)
   */
  def update(txnUuid : UUID, status: ProducerTransactionStatus, ttl : Int) : Unit = {
    //if update comes after close
    if (!map.exist(txnUuid) && status == ProducerTransactionStatus.updated)
      return

    if (map.exist(txnUuid)){
      if (map.get(txnUuid)._1 == ProducerTransactionStatus.finalCheckpoint) {
        return
      }
    }
    status match {
      case ProducerTransactionStatus.updated =>
        map.put(txnUuid, (status, ttl))

      case ProducerTransactionStatus.opened =>
        map.put(txnUuid, (status, ttl))

      case ProducerTransactionStatus.finalCheckpoint =>
        map.put(txnUuid, (status, -1)) //just ignore ttl because transaction is closed

      case ProducerTransactionStatus.cancelled =>
        map.remove(txnUuid)
    }
  }

  /**
   * Iterator on transaction buffer entry set
   * @return Iterator
   */
  def getIterator() = map.entrySetIterator()
}
