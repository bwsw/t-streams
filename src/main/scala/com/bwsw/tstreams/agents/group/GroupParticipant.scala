package com.bwsw.tstreams.agents.group

import com.bwsw.tstreams.storage.StorageClient

/**
  * Trait which can be implemented by any producer/consumer to apply group checkpoint
  */
trait GroupParticipant {

  private[tstreams] def getAgentName(): String

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
