package com.bwsw.tstreams.agents.consumer.subscriber_v2

import com.bwsw.tstreams.common.{InMemoryQueue, AbstractQueue}

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
    def generateQueueObject(id: Int): QueueType = ???
  }

  /**
    * InMemory Queues factory
    */
  class InMemory extends Abstract {
    override def generateQueueObject(id: Int): QueueType
      = new InMemoryQueue[QueueItemType]()
  }

  /**
    * Persistent Chronicle Queues factory
 *
    * @param dir
    */
  class Persistent(dir: String) extends Abstract {
    override def generateQueueObject(id: Int): QueueType
      = new TransactionStatePersistentQueue(java.nio.file.Paths.get(dir, id.toString).toString)
  }
}