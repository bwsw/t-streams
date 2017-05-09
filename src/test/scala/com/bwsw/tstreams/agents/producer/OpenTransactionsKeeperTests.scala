package com.bwsw.tstreams.agents.producer


import com.bwsw.tstreams.agents.group.ProducerCheckpointInfo
import com.bwsw.tstreams.testutils.LocalGeneratorCreator
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 28.08.16.
  */
class OpenTransactionsKeeperTests extends FlatSpec with Matchers {

  var ctr: Int = 0

  class TransactionStub extends IProducerTransaction {
    var lastMethod: String = null

    override def finalizeDataSend(): Unit = {}

    override def cancel(): Unit = {
      lastMethod = "cancel"
    }

    override def isClosed(): Boolean = false

    override def checkpoint(isSynchronous: Boolean): Unit = {
      lastMethod = "checkpoint"
    }

    override def updateTransactionKeepAliveState(): Unit = {
      ctr += 1
    }

    override def getTransactionInfo(): ProducerCheckpointInfo = null

    override def getTransactionID(): Long = LocalGeneratorCreator.getTransaction()

    override def markAsClosed(): Unit = {}

    override def send(obj: Array[Byte]): Unit = {}
  }

  class TransactionStubBadTransactionID extends TransactionStub {
    override def getTransactionID(): Long = 0
  }

  it should "allow add and get transactions to it" in {
    val keeper = new OpenTransactionsKeeper()
    keeper.put(0, new TransactionStub)
    keeper.getTransactionOption(0).isDefined shouldBe true
    keeper.getTransactionOption(1).isDefined shouldBe false
  }

  it should "handle all variants of awaitMaterialized" in {
    val keeper = new OpenTransactionsKeeper()
    val t = new TransactionStub
    keeper.put(0, t)
    keeper.handlePreviousOpenTransaction(0, NewProducerTransactionPolicy.CheckpointIfOpened)()
    t.lastMethod shouldBe "checkpoint"
    keeper.handlePreviousOpenTransaction(0, NewProducerTransactionPolicy.CancelIfOpened)()
    t.lastMethod shouldBe "cancel"
    (try {
      keeper.handlePreviousOpenTransaction(0, NewProducerTransactionPolicy.ErrorIfOpened)()
      false
    } catch {
      case e: IllegalStateException =>
        true
    }) shouldBe true
  }

  it should "handle for all keys do properly" in {
    val keeper = new OpenTransactionsKeeper()
    keeper.put(0, new TransactionStub)
    keeper.put(1, new TransactionStub)
    keeper.put(2, new TransactionStub)
    keeper.forallKeysDo[Unit]((p: Int, t: IProducerTransaction) => t.updateTransactionKeepAliveState())
    ctr shouldBe 3
  }

  it should "correctly raise an exception if master returned a transaction with ID which is less than the last one" in {
    val keeper = new OpenTransactionsKeeper()
    keeper.put(0, new TransactionStub)
    intercept[MasterInconsistencyException] {
      keeper.put(0, new TransactionStubBadTransactionID)
    }
  }

}
