package agents.producer


import com.bwsw.tstreams.agents.group.ProducerCheckpointInfo
import com.bwsw.tstreams.agents.producer.{IProducerTransaction, NewTransactionProducerPolicy, OpenTransactionsKeeper}
import org.scalatest.{FlatSpec, Matchers}
import testutils.LocalGeneratorCreator

/**
  * Created by Ivan Kudryavtsev on 28.08.16.
  */
class OpenTransactionsKeeperTests extends FlatSpec with Matchers {

  var ctr: Int = 0

  class TransactionStub extends IProducerTransaction[String] {
    var lastMethod: String = null

    override def awaitMaterialized(): Unit = {}

    override def finalizeDataSend(): Unit = {}

    override def cancel(): Unit = {
      lastMethod = "cancel"
    }

    override def send(obj: String): Unit = {}

    override def isClosed(): Boolean = false

    override def checkpoint(isSynchronous: Boolean): Unit = {
      lastMethod = "checkpoint"
    }

    override def updateTransactionKeepAliveState(): Unit = {
      ctr += 1
    }

    override def getTransactionInfo(): ProducerCheckpointInfo = null

    override def getTransactionID(): Long = LocalGeneratorCreator.getTransaction()

    override def makeMaterialized(): Unit = {}

    override def markAsClosed(): Unit = {}
  }

  it should "allow add and get transactions to it" in {
    val keeper = new OpenTransactionsKeeper[String]()
    keeper.put(0, new TransactionStub)
    keeper.getTransactionOption(0).isDefined shouldBe true
    keeper.getTransactionOption(1).isDefined shouldBe false
  }

  it should "handle all variants of awaitMaterialized" in {
    val keeper = new OpenTransactionsKeeper[String]()
    val t = new TransactionStub
    keeper.put(0, t)
    keeper.awaitOpenTransactionMaterialized(0, NewTransactionProducerPolicy.CheckpointIfOpened)()
    t.lastMethod shouldBe "checkpoint"
    keeper.awaitOpenTransactionMaterialized(0, NewTransactionProducerPolicy.CancelIfOpened)()
    t.lastMethod shouldBe "cancel"
    (try {
      keeper.awaitOpenTransactionMaterialized(0, NewTransactionProducerPolicy.ErrorIfOpened)()
      false
    } catch {
      case e: IllegalStateException =>
        true
    }) shouldBe true
  }

  it should "handle for all keys do properly" in {
    val keeper = new OpenTransactionsKeeper[String]()
    keeper.put(0, new TransactionStub)
    keeper.put(1, new TransactionStub)
    keeper.put(2, new TransactionStub)
    keeper.forallKeysDo[Unit]((p: Int, t: IProducerTransaction[String]) => t.updateTransactionKeepAliveState())
    ctr shouldBe 3
  }

}
