package agents.subscriber


import java.util.concurrent.TimeUnit

import com.bwsw.tstreams.agents.consumer.subscriber.{QueueBuilder, TransactionBuffer, TransactionState}
import com.bwsw.tstreams.coordination.messages.state.TransactionStatus
import com.datastax.driver.core.utils.UUIDs
import org.scalatest.{FlatSpec, Matchers}

object TransactionBufferTests {
  val OPENED = 0
  val UPDATE = 1
  val PRE = 2
  val POST = 3
  val CANCEL = 4
  val UPDATE_TTL = 20
  val OPEN_TTL = 10
  def generateAllStates(): Array[TransactionState] = {
    val uuid = UUIDs.timeBased()
    Array[TransactionState](
      TransactionState(uuid, 0, 0, 0, -1, TransactionStatus.opened, OPEN_TTL),
      TransactionState(uuid, 0, 0, 0, -1, TransactionStatus.update, UPDATE_TTL),
      TransactionState(uuid, 0, 0, 0, -1, TransactionStatus.preCheckpoint, 10),
      TransactionState(uuid, 0, 0, 0, -1, TransactionStatus.postCheckpoint, 10),
      TransactionState(uuid, 0, 0, 0, -1, TransactionStatus.cancel, 10))
  }
}
/**
  * Created by Ivan Kudryavtsev on 19.08.16.
  */
class TransactionBufferTests extends FlatSpec with Matchers {

  val OPENED  = TransactionBufferTests.OPENED
  val UPDATE  = TransactionBufferTests.UPDATE
  val PRE     = TransactionBufferTests.PRE
  val POST    = TransactionBufferTests.POST
  val CANCEL  = TransactionBufferTests.CANCEL
  val UPDATE_TTL  = TransactionBufferTests.UPDATE_TTL
  val OPEN_TTL    = TransactionBufferTests.OPEN_TTL

  def generateAllStates() = TransactionBufferTests.generateAllStates()


  it should "be created" in {
    val b = new TransactionBuffer(new QueueBuilder.InMemory().generateQueueObject(0))
  }

  it should "avoid addition of update state if no previous state" in {
    val b = new TransactionBuffer(new QueueBuilder.InMemory().generateQueueObject(0))
    val ts0 = generateAllStates()
    b.update(ts0(UPDATE))
    b.getState(ts0(UPDATE).uuid).isDefined shouldBe false
  }

  it should "avoid addition of pre state if no previous state" in {
    val b = new TransactionBuffer(new QueueBuilder.InMemory().generateQueueObject(0))
    val ts0 = generateAllStates()
    b.update(ts0(PRE))
    b.getState(ts0(PRE).uuid).isDefined shouldBe false
  }

  it should "avoid addition of post state if no previous state" in {
    val b = new TransactionBuffer(new QueueBuilder.InMemory().generateQueueObject(0))
    val ts0 = generateAllStates()
    b.update(ts0(POST))
    b.getState(ts0(POST).uuid).isDefined shouldBe false
  }

  it should "avoid addition of cancel state if no previous state" in {
    val b = new TransactionBuffer(new QueueBuilder.InMemory().generateQueueObject(0))
    val ts0 = generateAllStates()
    b.update(ts0(CANCEL))
    b.getState(ts0(CANCEL).uuid).isDefined shouldBe false
  }

  it should "avoid addition of ts0 after ts1" in {
    val b = new TransactionBuffer(new QueueBuilder.InMemory().generateQueueObject(0))
    val ts0 = generateAllStates()
    val ts1 = generateAllStates()
    b.update(ts1(OPENED))
    b.update(ts0(OPENED))
    b.getState(ts0(OPENED).uuid).isDefined shouldBe false
    b.getState(ts1(OPENED).uuid).isDefined shouldBe true
  }


  it should "allow to place opened state" in {
    val b = new TransactionBuffer(new QueueBuilder.InMemory().generateQueueObject(0))
    val ts0 = generateAllStates()
    b.update(ts0(OPENED))
    b.getState(ts0(OPENED).uuid).isDefined shouldBe true
  }

  it should "move from opened to updated without problems, state should be opened, ttl must be changed" in {
    val b = new TransactionBuffer(new QueueBuilder.InMemory().generateQueueObject(0))
    val ts0 = generateAllStates()
    b.update(ts0(OPENED))
    b.update(ts0(UPDATE))
    b.getState(ts0(UPDATE).uuid).isDefined shouldBe true
    b.getState(ts0(UPDATE).uuid).get.ttl shouldBe UPDATE_TTL
    b.getState(ts0(UPDATE).uuid).get.state shouldBe TransactionStatus.opened
  }

  it should "move from opened to cancelled" in {
    val b = new TransactionBuffer(new QueueBuilder.InMemory().generateQueueObject(0))
    val ts0 = generateAllStates()
    b.update(ts0(OPENED))
    b.update(ts0(CANCEL))
    b.getState(ts0(UPDATE).uuid).isDefined shouldBe false
  }

  it should "move from opened to preCheckpoint to Cancel" in {
    val b = new TransactionBuffer(new QueueBuilder.InMemory().generateQueueObject(0))
    val ts0 = generateAllStates()
    b.update(ts0(OPENED))
    b.update(ts0(PRE))
    b.getState(ts0(PRE).uuid).isDefined shouldBe true
    b.getState(ts0(PRE).uuid).get.state shouldBe TransactionStatus.preCheckpoint
    b.getState(ts0(PRE).uuid).get.ttl shouldBe -1
    b.update(ts0(CANCEL))
    b.getState(ts0(CANCEL).uuid).isDefined shouldBe false
  }

  it should "move from opened to preCheckpoint to postCheckpoint" in {
    val b = new TransactionBuffer(new QueueBuilder.InMemory().generateQueueObject(0))
    val ts0 = generateAllStates()
    b.update(ts0(OPENED))
    b.update(ts0(PRE))
    b.getState(ts0(PRE).uuid).isDefined shouldBe true
    b.getState(ts0(PRE).uuid).get.state shouldBe TransactionStatus.preCheckpoint
    b.getState(ts0(PRE).uuid).get.ttl shouldBe -1
    b.update(ts0(POST))
    b.getState(ts0(POST).uuid).isDefined shouldBe true
    b.getState(ts0(POST).uuid).get.ttl shouldBe -1
  }

  it should "move from opened to preCheckpoint update stay in preCheckpoint" in {
    val b = new TransactionBuffer(new QueueBuilder.InMemory().generateQueueObject(0))
    val ts0 = generateAllStates()
    b.update(ts0(OPENED))
    b.update(ts0(PRE))
    b.getState(ts0(PRE).uuid).isDefined shouldBe true
    b.getState(ts0(PRE).uuid).get.state shouldBe TransactionStatus.preCheckpoint
    b.getState(ts0(PRE).uuid).get.ttl shouldBe -1
    b.update(ts0(UPDATE))
    b.getState(ts0(PRE).uuid).isDefined shouldBe true
    b.getState(ts0(PRE).uuid).get.state shouldBe TransactionStatus.preCheckpoint
    b.getState(ts0(PRE).uuid).get.ttl shouldBe -1
  }

  it should "move to preCheckpoint impossible" in {
    val b = new TransactionBuffer(new QueueBuilder.InMemory().generateQueueObject(0))
    val ts0 = generateAllStates()
    b.update(ts0(PRE))
    b.getState(ts0(PRE).uuid).isDefined shouldBe false
  }

  it should "move to postCheckpoint impossible" in {
    val b = new TransactionBuffer(new QueueBuilder.InMemory().generateQueueObject(0))
    val ts0 = generateAllStates()
    b.update(ts0(POST))
    b.getState(ts0(POST).uuid).isDefined shouldBe false
  }

  it should "move to cancel impossible" in {
    val b = new TransactionBuffer(new QueueBuilder.InMemory().generateQueueObject(0))
    val ts0 = generateAllStates()
    b.update(ts0(CANCEL))
    b.getState(ts0(CANCEL).uuid).isDefined shouldBe false
  }

  it should "move to update impossible" in {
    val b = new TransactionBuffer(new QueueBuilder.InMemory().generateQueueObject(0))
    val ts0 = generateAllStates()
    b.update(ts0(UPDATE))
    b.getState(ts0(UPDATE).uuid).isDefined shouldBe false
  }

  it should "signal for one completed txn" in {
    val q = new QueueBuilder.InMemory().generateQueueObject(0)
    val b = new TransactionBuffer(q)
    val ts0 = generateAllStates()
    val ts1 = generateAllStates()
    b.update(ts0(OPENED))
    b.update(ts0(PRE))
    b.update(ts0(POST))
    b.signalCompleteTransactions()
    val r = q.get(1, TimeUnit.MILLISECONDS)
    r.size shouldBe 1
    r.head.uuid shouldBe ts0(OPENED).uuid
    b.getState(ts0(OPENED).uuid).isDefined shouldBe false
  }


  it should "signal for two completed txn" in {
    val q = new QueueBuilder.InMemory().generateQueueObject(0)
    val b = new TransactionBuffer(q)
    val ts0 = generateAllStates()
    val ts1 = generateAllStates()
    b.update(ts0(OPENED))
    b.update(ts1(OPENED))
    b.update(ts0(PRE))
    b.update(ts1(PRE))
    b.update(ts0(POST))
    b.update(ts1(POST))
    b.signalCompleteTransactions()
    val r = q.get(1, TimeUnit.MILLISECONDS)
    r.size shouldBe 2
    r.head.uuid shouldBe ts0(OPENED).uuid
    r.tail.head.uuid shouldBe ts1(OPENED).uuid
  }

  it should "signal for first incomplete, second completed" in {
    val q = new QueueBuilder.InMemory().generateQueueObject(0)
    val b = new TransactionBuffer(q)
    val ts0 = generateAllStates()
    val ts1 = generateAllStates()
    b.update(ts0(OPENED))
    b.update(ts1(OPENED))
    b.update(ts0(PRE))
    b.update(ts1(PRE))
    b.update(ts1(POST))
    b.signalCompleteTransactions()
    val r = q.get(1, TimeUnit.MILLISECONDS)
    r shouldBe null
  }

  it should "signal for first incomplete, second incomplete" in {
    val q = new QueueBuilder.InMemory().generateQueueObject(0)
    val b = new TransactionBuffer(q)
    val ts0 = generateAllStates()
    val ts1 = generateAllStates()
    b.update(ts0(OPENED))
    b.update(ts1(OPENED))
    b.update(ts0(PRE))
    b.update(ts1(PRE))
    b.signalCompleteTransactions()
    val r = q.get(1, TimeUnit.MILLISECONDS)
    r shouldBe null
  }

  it should "signal for first complete, second incomplete" in {
    val q = new QueueBuilder.InMemory().generateQueueObject(0)
    val b = new TransactionBuffer(q)
    val ts0 = generateAllStates()
    val ts1 = generateAllStates()
    b.update(ts0(OPENED))
    b.update(ts1(OPENED))
    b.update(ts0(PRE))
    b.update(ts0(POST))
    b.signalCompleteTransactions()
    val r = q.get(1, TimeUnit.MILLISECONDS)
    r.size shouldBe 1
  }

  it should "signal for first incomplete, second complete, first complete" in {
    val q = new QueueBuilder.InMemory().generateQueueObject(0)
    val b = new TransactionBuffer(q)
    val ts0 = generateAllStates()
    val ts1 = generateAllStates()
    b.update(ts0(OPENED))
    b.update(ts1(OPENED))
    b.update(ts1(PRE))
    b.update(ts0(PRE))
    b.update(ts1(POST))
    b.update(ts0(POST))
    b.signalCompleteTransactions()
    val r = q.get(1, TimeUnit.MILLISECONDS)
    r.size shouldBe 2
    r.head.uuid shouldBe ts0(OPENED).uuid
    r.tail.head.uuid shouldBe ts1(OPENED).uuid
  }

  it should "expire" in {
    val q = new QueueBuilder.InMemory().generateQueueObject(0)
    val b = new TransactionBuffer(q)
    val uuid = UUIDs.timeBased()
    b.update(TransactionState(uuid, 0, 0, 0, -1, TransactionStatus.opened, 1))
    Thread.sleep(500)
    b.getState(uuid).isDefined shouldBe true
    Thread.sleep(500)
    b.getState(uuid).isDefined shouldBe false
  }

}