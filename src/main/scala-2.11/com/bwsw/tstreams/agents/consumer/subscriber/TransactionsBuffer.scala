package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.UUID
import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreams.coordination.pubsub.messages.ProducerTransactionStatus
import ProducerTransactionStatus._
import com.bwsw.tstreams.coordination.pubsub.messages.ProducerTransactionStatus

/**
 * Buffer for maintaining consumed transactions in memory
 */
class TransactionsBuffer {
  private val map : SortedExpiringMap[UUID, (ProducerTransactionStatus, Long)] =
    new SortedExpiringMap(new UUIDComparator, new SubscriberExpirationPolicy)
  private val lock = new ReentrantLock(true)

  /**
   * Update transaction buffer
    *
    * @param txnUuid Transaction uuid
   * @param status Transaction status
   * @param ttl Transaction ttl(time of expiration)
   */
  def update(txnUuid : UUID, status: ProducerTransactionStatus, ttl : Int) : Unit = {
    lock.lock()

    //if txn is not opened we'll just wait open event
    if (!map.exist(txnUuid) && status != ProducerTransactionStatus.opened) {
      lock.unlock()
      return
    }

    //if txn closed we'll just ignore events
    if (map.exist(txnUuid)){
      if (map.get(txnUuid)._1 == ProducerTransactionStatus.finalCheckpoint) {
        lock.unlock()
        return
      }
    }

    status match {
      case ProducerTransactionStatus.updated =>
        map.put(txnUuid, (status, ttl))

      case ProducerTransactionStatus.opened =>
        map.put(txnUuid, (status, ttl))

      case ProducerTransactionStatus.preCheckpoint =>
        //ignore ttl, preCheckpoint will be resolved by another thread
        map.put(txnUuid, (status, -1))

      case ProducerTransactionStatus.finalCheckpoint =>
        //just ignore ttl because transaction is closed
        map.put(txnUuid, (status, -1))

      case ProducerTransactionStatus.cancelled =>
        map.remove(txnUuid)
    }

    lock.unlock()
  }

  /**
   * Iterator on transaction buffer entry set
    *
    * @return Iterator
   */
  def getIterator() = map.entrySetIterator()
}
