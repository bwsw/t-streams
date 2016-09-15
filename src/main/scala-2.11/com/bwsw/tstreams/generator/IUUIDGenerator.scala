package com.bwsw.tstreams.generator

import java.util.UUID

/**
  * Trait for producer/consumer transaction unique UUID generating
  */
trait IUUIDGenerator {
  /**
    * @return UUID
    */
  def getTimeUUID(): UUID

  /**
    * @return UUID based on timestamp
    */
  def getTimeUUID(timestamp: Long): UUID
}
