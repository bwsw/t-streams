package com.bwsw.tstreams.agents.consumer.subscriber_v2

import com.bwsw.tstreams.common.{InMemoryQueue, AbstractQueue}

/**
  * Represents factory which generates queues
  */
object QueueBuilder {

  /**
    * Abstract factory
    */
  trait Abstract {
    def generateQueueObject(partition: Int): java.util.concurrent.BlockingQueue[String] = ???
  }

  /**
    * InMemory Queues factory
    */
  class InMemory extends Abstract {
    override def generateQueueObject(partition: Int): AbstractQueue[List[TransactionState]]
      = new InMemoryQueue[List[TransactionState]]()
  }

  /**
    * Persistent Chronicle Queues factory
    * @param dir
    */
  class Persistent(dir: String) extends Abstract {
    override def generateQueueObject(partition: Int): AbstractQueue[List[TransactionState]]
      = new TransactionStatePersistentQueue(java.nio.file.Paths.get(dir, partition.toString).toString)
  }
}