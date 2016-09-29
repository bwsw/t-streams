package agents.subscriber

import java.util.concurrent.TimeUnit

import com.bwsw.tstreams.agents.consumer.subscriber.{QueueBuilder, TransactionBuffer, TransactionBufferWorker}
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 20.08.16.
  */
class TransactionBufferWorkerTests extends FlatSpec with Matchers {
  val ts0 = TransactionBufferTests.generateAllStates()
  val ts1 = TransactionBufferTests.generateAllStates()

  it should "Do combine update and signal and produce output to queue 2 items with 1 state each" in {
    val q = new QueueBuilder.InMemory().generateQueueObject(0)
    val b = new TransactionBuffer(q)
    val w = new TransactionBufferWorker()
    w.assign(0, b)

    w.update(ts0(TransactionBufferTests.OPENED))
    w.update(ts1(TransactionBufferTests.OPENED))

    w.update(ts0(TransactionBufferTests.PRE))
    w.update(ts1(TransactionBufferTests.PRE))

    w.update(ts0(TransactionBufferTests.POST))
    w.update(ts1(TransactionBufferTests.POST))

    val itm0 = q.get(100, TimeUnit.MILLISECONDS)
    itm0.size shouldBe 1
    val itm1 = q.get(100, TimeUnit.MILLISECONDS)
    itm1.size shouldBe 1
    itm0.head.transactionID shouldBe ts0(TransactionBufferTests.OPENED).transactionID
    itm1.head.transactionID shouldBe ts1(TransactionBufferTests.OPENED).transactionID
    w.stop()
  }

  it should "Do combine update and signal and produce output to queue 1 item with 2 states" in {
    val q = new QueueBuilder.InMemory().generateQueueObject(0)
    val b = new TransactionBuffer(q)
    val w = new TransactionBufferWorker()
    w.assign(0, b)

    w.update(ts0(TransactionBufferTests.OPENED))
    w.update(ts1(TransactionBufferTests.OPENED))

    w.update(ts0(TransactionBufferTests.PRE))
    w.update(ts1(TransactionBufferTests.PRE))

    w.update(ts1(TransactionBufferTests.POST))
    w.update(ts0(TransactionBufferTests.POST))

    val itm0 = q.get(100, TimeUnit.MILLISECONDS)
    itm0.size shouldBe 2
    itm0.head.transactionID shouldBe ts0(TransactionBufferTests.OPENED).transactionID
    itm0.tail.head.transactionID shouldBe ts1(TransactionBufferTests.OPENED).transactionID

    val itm1 = q.get(100, TimeUnit.MILLISECONDS)
    itm1 shouldBe null
    w.stop()
  }

  it should "raise exception after stop" in {
    val q = new QueueBuilder.InMemory().generateQueueObject(0)
    val b = new TransactionBuffer(q)
    val w = new TransactionBufferWorker()
    w.assign(0, b)
    w.update(ts0(TransactionBufferTests.OPENED))
    w.stop()
    val flag: Boolean = {
      try {
        w.update(ts0(TransactionBufferTests.UPDATE))
        false
      } catch {
        case e: RuntimeException =>
          true
      }
    }
    flag shouldBe true
  }

  it should "raise exception if second assignment to the same partition" in {
    val q = new QueueBuilder.InMemory().generateQueueObject(0)
    val b = new TransactionBuffer(q)
    val w = new TransactionBufferWorker()
    w.assign(0, b)
    val flag: Boolean = {
      try {
        w.assign(0, b)
        false
      } catch {
        case e: RuntimeException =>
          true
      }
    }
    flag shouldBe true
  }

  it should "not raise exception if second assignment to another partition" in {
    val q = new QueueBuilder.InMemory().generateQueueObject(0)
    val b = new TransactionBuffer(q)
    val w = new TransactionBufferWorker()
    w.assign(0, b)
    val flag: Boolean = {
      try {
        w.assign(1, b)
        true
      } catch {
        case e: RuntimeException =>
          false
      }
    }
    flag shouldBe true
  }


}