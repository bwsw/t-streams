package ut

import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.{ProducerTransactionKey, ProducerTransactionValue}
import org.scalatest.{FlatSpec, Matchers}
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates

class TransactionMetaServiceTest
  extends FlatSpec
    with Matchers {

  "Key" should "be serialized/deserialized" in {
    val key = ProducerTransactionKey(1, 10, 15L)
    ProducerTransactionKey.fromByteArray(key.toByteArray) shouldBe key
  }

  it should "be serialized/deserialized with negative stream" in {
    val key = ProducerTransactionKey(-1, 10, 15L)
    ProducerTransactionKey.fromByteArray(key.toByteArray) shouldBe key
  }

  it should "be serialized/deserialized with negative partition" in {
    val key = ProducerTransactionKey(1, -10, 15L)
    ProducerTransactionKey.fromByteArray(key.toByteArray) shouldBe key
  }

  it should "be serialized/deserialized with negative transaction" in {
    val key = ProducerTransactionKey(1, 10, -15L)
    ProducerTransactionKey.fromByteArray(key.toByteArray) shouldBe key
  }

  "ProducerTransaction" should "be serialized/deserialized" in {
    val producerTransaction = ProducerTransactionValue(TransactionStates.Opened, 10, Long.MaxValue, Long.MaxValue)
    ProducerTransactionValue.fromByteArray(producerTransaction.toByteArray) shouldBe producerTransaction
  }

  it should "be serialized/deserialized with negative quantity" in {
    val producerTransaction = ProducerTransactionValue(TransactionStates.Opened, -10, Long.MaxValue, Long.MaxValue)
    ProducerTransactionValue.fromByteArray(producerTransaction.toByteArray) shouldBe producerTransaction
  }

  it should "be serialized/deserialized with negative ttl" in {
    val producerTransaction = ProducerTransactionValue(TransactionStates.Opened, -10, Long.MinValue, Long.MaxValue)
    ProducerTransactionValue.fromByteArray(producerTransaction.toByteArray) shouldBe producerTransaction
  }
}
