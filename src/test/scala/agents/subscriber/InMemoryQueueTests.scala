package agents.subscriber

import java.util.concurrent.TimeUnit

import com.bwsw.tstreams.agents.consumer.subscriber.TransactionState
import com.bwsw.tstreams.common.InMemoryQueue
import com.bwsw.tstreams.coordination.messages.state.TransactionStatus
import org.scalatest.{FlatSpec, Matchers}
import testutils.LocalGeneratorCreator

/**
  * Created by Ivan Kudryavtsev on 19.08.16.
  */
class InMemoryQueueTests extends FlatSpec with Matchers {

  it should "created" in {
    val q = new InMemoryQueue[List[TransactionState]]()
  }

  it should "allow to put/get list" in {
    val q = new InMemoryQueue[List[TransactionState]]()
    val s = TransactionState(LocalGeneratorCreator.getTransaction(), 0, 1, 1, 1, TransactionStatus.opened, 1)
    q.put(List(s))
    val g: List[TransactionState] = q.get(1, TimeUnit.SECONDS)
    g.size shouldBe 1
    g.head.transactionID shouldBe s.transactionID
  }

  it should "return null no data in list" in {
    val q = new InMemoryQueue[List[TransactionState]]()
    val g: List[TransactionState] = q.get(1, TimeUnit.SECONDS)
    g shouldBe null
  }

  it should "lock if no data in list" in {
    val q = new InMemoryQueue[List[TransactionState]]()
    val start = System.currentTimeMillis()
    val g: List[TransactionState] = q.get(10, TimeUnit.MILLISECONDS)
    val end = System.currentTimeMillis()
    end - start > 9 shouldBe true
  }

  it should "work with empty lists" in {
    val q = new InMemoryQueue[List[TransactionState]]()
    q.put(Nil)
    val g: List[TransactionState] = q.get(10, TimeUnit.MILLISECONDS)
    g shouldBe Nil
  }

  it should "keep all items" in {
    val q = new InMemoryQueue[List[TransactionState]]()
    val N = 1000
    for (i <- 0 until N) {
      q.put(Nil)
    }
    var ctr: Int = 0
    var elt: List[TransactionState] = q.get(1, TimeUnit.MILLISECONDS)
    while (elt != null) {
      ctr += 1
      elt = q.get(1, TimeUnit.MILLISECONDS)
    }
    ctr shouldBe N
  }

  it should "correctly work with inFlight count" in {
    val q = new InMemoryQueue[List[TransactionState]]()
    val N = 1000
    for (i <- 0 until N) {
      q.put(Nil)
    }
    q.getInFlight() shouldBe 1000
    for (i <- 0 until N) {
      q.get(1, TimeUnit.MILLISECONDS)
    }
    q.getInFlight() shouldBe 0

    q.get(1, TimeUnit.MILLISECONDS)
    q.getInFlight() shouldBe 0

  }

  it should "be signalled" in {
    val q = new InMemoryQueue[List[TransactionState]]()
    val t = new Thread(() => {
      Thread.sleep(10)
      q.put(Nil)
    })
    t.start
    val start = System.currentTimeMillis()
    q.get(100, TimeUnit.MILLISECONDS)
    val end = System.currentTimeMillis()
    end - start < 50 shouldBe true
  }
}
