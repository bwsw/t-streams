package agents.subscriber


import java.util.UUID

import com.bwsw.tstreams.agents.consumer.subscriber_v2.{QueueBuilder, TransactionBuffer, TransactionState}
import com.bwsw.tstreams.coordination.messages.state.TransactionStatus
import com.datastax.driver.core.utils.UUIDs
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by ivan on 19.08.16.
  */
class TransactionBufferTests extends FlatSpec with Matchers {

  val OPENED  =0
  val UPDATE  =1
  val PRE     =2
  val POST    =3
  val CANCEL  =4
  val UPDATE_TTL = 20
  val OPEN_TTL = 10


  def generateAllStates(): Array[TransactionState] = {
    val uuid = UUIDs.timeBased()
    Array[TransactionState](
      TransactionState(uuid, 0, 0, -1, TransactionStatus.opened, OPEN_TTL),
      TransactionState(uuid, 0, 0, -1, TransactionStatus.update, UPDATE_TTL),
      TransactionState(uuid, 0, 0, -1, TransactionStatus.preCheckpoint, 10),
      TransactionState(uuid, 0, 0, -1, TransactionStatus.postCheckpoint, 10),
      TransactionState(uuid, 0, 0, -1, TransactionStatus.cancel, 10))
  }

  it should "be created" in {
    val b = new TransactionBuffer(new QueueBuilder.InMemory().generateQueueObject(0))
  }

  it should "avoid addition of update state if no previous state" in {
    val b = new TransactionBuffer(new QueueBuilder.InMemory().generateQueueObject(0))
    val ts0 = generateAllStates()
    b.update(ts0(UPDATE))
    b.getState(ts0(UPDATE).uuid).isDefined shouldBe false
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



}
