package agents.producer

import java.util.UUID

import com.bwsw.tstreams.agents.group.ProducerCheckpointInfo
import com.bwsw.tstreams.agents.producer.{NewTransactionProducerPolicy, IProducerTransaction, OpenTransactionsKeeper}
import com.datastax.driver.core.utils.UUIDs
import org.scalatest.{Matchers, FlatSpec}

/**
  * Created by Ivan Kudryavtsev on 28.08.16.
  */
class OpenTransactionsKeeperTests   extends FlatSpec with Matchers  {

  var ctr: Int = 0

  class TxnStub extends IProducerTransaction[String] {
    var lastMethod: String = null
    override def awaitMaterialized(): Unit = {}
    override def finalizeDataSend(): Unit = {}
    override def cancel(): Unit = {lastMethod = "cancel"}
    override def send(obj: String): Unit = {}
    override def isClosed(): Boolean = false
    override def checkpoint(isSynchronous: Boolean): Unit = {lastMethod = "checkpoint"}
    override def updateTxnKeepAliveState(): Unit = { ctr += 1}
    override def getTransactionInfo(): ProducerCheckpointInfo = null
    override def getTransactionUUID(): UUID = UUIDs.timeBased()
    override def makeMaterialized(): Unit = {}
  }

  it should "allow add and get transactions to it" in {
    val otk = new OpenTransactionsKeeper[String]()
    otk.put(0, new TxnStub)
    otk.getTransactionOption(0).isDefined shouldBe true
    otk.getTransactionOption(1).isDefined shouldBe false
  }

  it should "handle all variants of awaitMaterialized" in {
    val otk = new OpenTransactionsKeeper[String]()
    val t = new TxnStub
    otk.put(0, t)
    otk.awaitOpenTransactionMaterialized(0, NewTransactionProducerPolicy.CheckpointIfOpened)()
    t.lastMethod shouldBe "checkpoint"
    otk.awaitOpenTransactionMaterialized(0, NewTransactionProducerPolicy.CancelIfOpened)()
    t.lastMethod shouldBe "cancel"
    (try {
      otk.awaitOpenTransactionMaterialized(0, NewTransactionProducerPolicy.ErrorIfOpened)()
      false
    } catch {
      case e: IllegalStateException =>
        true
    }) shouldBe true
  }

  it should "handle for all keys do properly" in {
    val otk = new OpenTransactionsKeeper[String]()
    otk.put(0, new TxnStub)
    otk.put(1, new TxnStub)
    otk.put(2, new TxnStub)
    otk.forallKeysDo[Unit]((p: Int, t: IProducerTransaction[String]) => t.updateTxnKeepAliveState())
    ctr shouldBe 3
  }

}
