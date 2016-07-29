package com.bwsw.tstreams.generator

import java.util.UUID

import com.datastax.driver.core.utils.UUIDs

/**
  * Entity for generating new transaction time UUID
  */
class LocalTimeUUIDGenerator extends IUUIDGenerator {
  /**
    * @return Transaction UUID
    */
  override def getTimeUUID(): UUID = UUIDs.timeBased()

  /**
    * @return UUID based on timestamp
    */
  override def getTimeUUID(timestamp: Long): UUID = UUIDs.startOf(timestamp)
}