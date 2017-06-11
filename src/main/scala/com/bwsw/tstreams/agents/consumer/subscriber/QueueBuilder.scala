package com.bwsw.tstreams.agents.consumer.subscriber

import com.bwsw.tstreams.common.{MemoryQueue, Queue}
import com.bwsw.tstreamstransactionserver.protocol.TransactionState

/**
  * Represents factory which generates queues
  */
object QueueBuilder {

  type QueueItemType = List[TransactionState]
  type QueueType = Queue[QueueItemType]

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
    = new MemoryQueue[QueueItemType]()
  }

}