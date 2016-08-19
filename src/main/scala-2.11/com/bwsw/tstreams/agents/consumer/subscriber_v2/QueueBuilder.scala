package com.bwsw.tstreams.agents.consumer.subscriber_v2

import com.bwsw.tstreams.common.{InMemoryQueue, AbstractQueue}
import scala.util.Random

object QueueBuilder {
  trait Abstract {
    def generateQueueObject(): java.util.concurrent.BlockingQueue[String] = ???
  }

  class InMemory extends Abstract {
    override def generateQueueObject(): AbstractQueue[List[TransactionState]]
      = new InMemoryQueue[List[TransactionState]]()
  }

  class Persistent(dir: String) extends Abstract {
    val rand = new Random()
    override def generateQueueObject(): AbstractQueue[List[TransactionState]]
      = new TransactionStatePersistentQueue(java.nio.file.Paths.get(dir, rand.nextInt.toString).toString)
  }
}