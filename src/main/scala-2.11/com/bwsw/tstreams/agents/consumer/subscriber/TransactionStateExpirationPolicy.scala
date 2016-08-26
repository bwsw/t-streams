package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.UUID

import org.apache.commons.collections4.map.PassiveExpiringMap

/**
  * Policy
  * where expiration strategy based on records ttl
  */
class TransactionStateExpirationPolicy
  extends PassiveExpiringMap.ExpirationPolicy[UUID, TransactionState] {
  override def expirationTime(key: UUID, value: TransactionState): Long = {
    if (value.ttl < 0)
      -1 //just need to keep records without ttl
    else
      System.currentTimeMillis() + value.ttl * 1000L
  }
}
