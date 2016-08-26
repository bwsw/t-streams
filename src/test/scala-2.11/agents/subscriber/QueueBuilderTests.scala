package agents.subscriber

import java.util.UUID

import com.bwsw.tstreams.agents.consumer.subscriber.{QueueBuilder, TransactionState, TransactionStatePersistentQueue}
import com.bwsw.tstreams.common.InMemoryQueue
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by ivan on 19.08.16.
  */
class QueueBuilderTests extends FlatSpec with Matchers {
  it should "Return InMemoryQueue" in {
    new QueueBuilder.InMemory()
      .generateQueueObject(0)
      .isInstanceOf[InMemoryQueue[List[TransactionState]]] shouldBe true
  }

  it should "Return TransactionStatePersistentQueue" in {
    new QueueBuilder.Persistent(s"target/${UUID.randomUUID().toString}")
      .generateQueueObject(0)
      .isInstanceOf[TransactionStatePersistentQueue] shouldBe true
  }

}
