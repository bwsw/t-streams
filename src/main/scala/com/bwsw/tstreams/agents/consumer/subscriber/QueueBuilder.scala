package com.bwsw.tstreams.agents.consumer.subscriber

import com.bwsw.tstreams.common.{AbstractQueue, InMemoryQueue}
import com.bwsw.tstreams.proto.protocol.TransactionState

/**
  * Represents factory which generates queues
  */
object QueueBuilder {

  type QueueItemType = List[TransactionState]
  type QueueType = AbstractQueue[QueueItemType]

  /**
    * Abstract factory
    */
  trait Abstract {
    def generateQueueObject(id: Int): QueueType
  }

  /**
    * InMemory Queues factory
    */
  class InMemory extends Abstract {
    override def generateQueueObject(id: Int): QueueType
    = new InMemoryQueue[QueueItemType]()
  }

}