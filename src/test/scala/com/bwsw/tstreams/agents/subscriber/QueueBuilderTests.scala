package com.bwsw.tstreams.agents.subscriber

import com.bwsw.tstreams.agents.consumer.subscriber.{QueueBuilder, TransactionState, TransactionStatePersistentQueue}
import com.bwsw.tstreams.common.InMemoryQueue
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 19.08.16.
  */
class QueueBuilderTests extends FlatSpec with Matchers {
  it should "Return InMemoryQueue" in {
    new QueueBuilder.InMemory()
      .generateQueueObject(0)
      .isInstanceOf[InMemoryQueue[List[TransactionState]]] shouldBe true
  }

  it should "Return TransactionStatePersistentQueue" in {
    new QueueBuilder.Persistent(s"target/${System.currentTimeMillis().toString}")
      .generateQueueObject(0)
      .isInstanceOf[TransactionStatePersistentQueue] shouldBe true
  }

}
