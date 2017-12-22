package benchmark.utils

import com.bwsw.tstreamstransactionserver.rpc.{ProducerTransaction, TransactionStates}

import scala.collection.immutable.IndexedSeq

trait TransactionCreator {
  private val rand = new scala.util.Random()

  def createProducerTransactions(streamID: Int, partition: Int, _type: TransactionStates, count: Int) = {
    (0 until count).map(_ => createTransaction(streamID, partition, _type))
  }

  def createTransaction(streamID: Int, _partition: Int, _type: TransactionStates): ProducerTransaction = {
    new ProducerTransaction {
      override val transactionID: Long = System.nanoTime()

      override val state: TransactionStates = _type

      override val stream: Int = streamID

      override val ttl: Long = System.currentTimeMillis()

      override val quantity: Int = -1

      override val partition: Int = _partition
    }
  }

  def createTransaction(streamID: Int, _partition: Int, _type: TransactionStates, id: Long): ProducerTransaction = {
    new ProducerTransaction {
      override val transactionID: Long = id

      override val state: TransactionStates = _type

      override val stream: Int = streamID

      override val ttl: Long = System.currentTimeMillis()

      override val quantity: Int = -1

      override val partition: Int = _partition
    }
  }

  def createTransactionData(count: Int): IndexedSeq[Array[Byte]] = {
    (0 until count) map (_ => new Array[Byte](4))
  }
}
