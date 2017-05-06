package com.bwsw.tstreams.agents.group

import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreams.storage.StorageClient

/**
  * Trait which can be implemented by any producer/consumer to apply group checkpoint
  */
trait GroupParticipant {

  private[tstreams] def getAgentName(): String

  /**
    * Agent lock on any actions which has to do with checkpoint
    */
  private[tstreams] def getThreadLock(): ReentrantLock

  /**
    * Info to commit
    */
  private[tstreams] def getCheckpointInfoAndClear(): List[CheckpointInfo]

  private[tstreams] def getStorageClient(): StorageClient

}

/**
  * Agent which sends data into transactions
  */
trait SendingAgent {
  private[agents] def finalizeDataSend(): Unit
}
