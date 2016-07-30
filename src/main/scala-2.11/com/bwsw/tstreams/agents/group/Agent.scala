package com.bwsw.tstreams.agents.group

import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreams.metadata.MetadataStorage

/**
  * Trait which can be implemented by any producer/consumer to apply group checkpoint
  */
trait Agent {

  /**
    * Agent lock on any actions which has to do with checkpoint
    */
  def getThreadLock(): ReentrantLock

  /**
    * Info to commit
    */
  def getCheckpointInfoAndClear(): List[CheckpointInfo]

  /**
    * Metadata storage link for concrete agent
    */
  def getMetadataRef(): MetadataStorage
}
