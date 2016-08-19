package agents.subscriber

import java.util.UUID
import java.util.concurrent.TimeUnit

import com.bwsw.tstreams.agents.consumer.subscriber_v2.{TransactionStatePersistentQueue, TransactionState}
import com.bwsw.tstreams.coordination.messages.state.TransactionStatus
import org.scalatest.{Matchers, FlatSpec}

/**
  * Created by ivan on 19.08.16.
  */
class PersistentQueueTests extends FlatSpec with Matchers {
  it should "created" in {
    val dir = s"target/${UUID.randomUUID().toString}"
    val q = new TransactionStatePersistentQueue(dir)
    q.put(Nil)
    q.get(1, TimeUnit.SECONDS)
    java.nio.file.Paths.get(dir, "queue").toFile.exists() shouldBe true
  }

  it should "allow to put/get list" in {
    val q = new TransactionStatePersistentQueue(s"target/${UUID.randomUUID().toString}")
    val s = TransactionState(UUID.randomUUID(), 1, 1, 1, TransactionStatus.opened, 1)
    q.put(List(s))
    val g: List[TransactionState] = q.get(1, TimeUnit.SECONDS)
    g.size shouldBe 1
    g.head.uuid shouldBe s.uuid
  }

  it should "return null no data in list" in {
    val q = new TransactionStatePersistentQueue(s"target/${UUID.randomUUID().toString}")
    val g: List[TransactionState] = q.get(1, TimeUnit.SECONDS)
    g shouldBe null
  }

  it should "lock if no data in list" in {
    val q = new TransactionStatePersistentQueue(s"target/${UUID.randomUUID().toString}")
    val start = System.currentTimeMillis()
    val g: List[TransactionState] = q.get(10, TimeUnit.MILLISECONDS)
    val end = System.currentTimeMillis()
    end - start > 9 shouldBe true
  }

  it should "work with empty lists" in {
    val q = new TransactionStatePersistentQueue(s"target/${UUID.randomUUID().toString}")
    q.put(Nil)
    val g: List[TransactionState] = q.get(10, TimeUnit.MILLISECONDS)
    g shouldBe Nil
  }

  "InMemoryQueue" should "keep all items" in {
    val q = new TransactionStatePersistentQueue(s"target/${UUID.randomUUID().toString}")
    val N = 1000
    for(i <- 0 until N) {
      q.put(Nil)
    }
    var ctr: Int = 0
    var elt: List[TransactionState] = q.get(1, TimeUnit.MILLISECONDS)
    while(elt != null) {
      ctr += 1
      elt = q.get(1, TimeUnit.MILLISECONDS)
    }
    ctr shouldBe N
  }

  "InMemoryQueue" should "be signalled" in {
    val q = new TransactionStatePersistentQueue(s"target/${UUID.randomUUID().toString}")
    val t = new Thread(new Runnable {
      override def run(): Unit = {
        Thread.sleep(10)
        q.put(Nil)
      }
    })
    t.start
    val start = System.currentTimeMillis()
    q.get(200, TimeUnit.MILLISECONDS)
    val end = System.currentTimeMillis()
    println(end - start)
    end - start < 100 shouldBe true
  }
}
