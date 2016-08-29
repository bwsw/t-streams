package agents.integration

import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.common.ResettableCountDownLatch
import com.bwsw.tstreams.debug.GlobalHooks
import com.bwsw.tstreams.env.TSF_Dictionary
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{LocalGeneratorCreator, TestUtils}

/**
  * Created by Ivan Kudryavtsev on 05.08.16.
  */
class ProducerUpdateTaskTest  extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils  {

  val blockCheckpoint1 = new ResettableCountDownLatch(1)
  val blockCheckpoint2 = new ResettableCountDownLatch(1)
  var flag: Int = 0

  System.setProperty("DEBUG", "true")
  GlobalHooks.addHook(GlobalHooks.transactionUpdateTaskBegin, () =>{
    flag = 2
    blockCheckpoint1.countDown
  })

  GlobalHooks.addHook(GlobalHooks.transactionUpdateTaskEnd, () =>{
    flag = 3
    blockCheckpoint2.countDown
  })


  f.setProperty(TSF_Dictionary.Stream.NAME,"test_stream").
    setProperty(TSF_Dictionary.Stream.PARTITIONS,3).
    setProperty(TSF_Dictionary.Stream.TTL, 60 * 10).
    setProperty(TSF_Dictionary.Coordination.CONNECTION_TIMEOUT, 7).
    setProperty(TSF_Dictionary.Coordination.TTL, 7).
    setProperty(TSF_Dictionary.Producer.TRANSPORT_TIMEOUT, 5).
    setProperty(TSF_Dictionary.Producer.Transaction.TTL, 6).
    setProperty(TSF_Dictionary.Producer.Transaction.KEEP_ALIVE, 2).
    setProperty(TSF_Dictionary.Consumer.TRANSACTION_PRELOAD, 10).
    setProperty(TSF_Dictionary.Consumer.DATA_PRELOAD, 10)

  val producer = f.getProducer[String](
    name = "test_producer",
    txnGenerator = LocalGeneratorCreator.getGen(),
    converter = stringToArrayByteConverter,
    partitions = List(0,1,2),
    isLowPriority = false)


  "BasicProducer.checkpoint with delay in update (test latch in update)" should "complete in ordered way" in {
    blockCheckpoint1.setValue(1)
    blockCheckpoint2.setValue(1)
    val t = producer.newTransaction(policy = NewTransactionProducerPolicy.CheckpointIfOpened)
    t.send("data")
    blockCheckpoint1.await()
    t.checkpoint()
    flag = 1
    blockCheckpoint2.await()
    flag shouldBe 1
  }

  "BasicProducer.cancel with delay in update (test latch in update)" should "complete in ordered way" in {
    blockCheckpoint1.setValue(1)
    blockCheckpoint2.setValue(1)
    val t = producer.newTransaction(policy = NewTransactionProducerPolicy.CheckpointIfOpened)
    t.send("data")
    blockCheckpoint1.await()
    t.cancel()
    flag = 1
    blockCheckpoint2.await()
    flag shouldBe 1
  }

  override def afterAll(): Unit = {
    producer.stop()
    onAfterAll()
  }
}
