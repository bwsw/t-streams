package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.UUID

import com.bwsw.tstreams.common.SortedExpiringMap
import com.bwsw.tstreams.coordination.messages.state.TransactionStatus._
import org.apache.commons.collections4.map.PassiveExpiringMap

/**
  * Policy for [[SortedExpiringMap]]]
  * where expiration strategy based on records ttl
  */
class SubscriberExpirationPolicy extends PassiveExpiringMap.ExpirationPolicy[UUID, (ProducerTransactionStatus, Long)] {
  override def expirationTime(key: UUID, value: (ProducerTransactionStatus, Long)): Long = {
    val (_, ttl) = value
    if (ttl < 0)
      -1 //just need to keep records without ttl
    else
      System.currentTimeMillis() + ttl * 1000L
  }
}
