package benchmark.utils.writer

import benchmark.utils.{CsvWriter, TimeMeasure, TransactionCreator}
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates

import benchmark.Options._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class TransactionLifeCycleWriter(streamID: Int, partition: Int = 1) extends TransactionCreator with CsvWriter with TimeMeasure {
  def run(txnCount: Int, dataSize: Int, filename: String) {
    val client = clientBuilder
      .build()

    val data = createTransactionData(dataSize)

    implicit val context = ExecutionContext.Implicits.global

    var globalProgress = 1
    val result = (1 to txnCount).map(x => {

      val localProgress = (x.toDouble / txnCount * 100).round
      if (globalProgress == localProgress) {
        println(localProgress + "%")
        globalProgress += 1
      }


      val openedProducerTransaction = createTransaction(streamID, partition, TransactionStates.Opened)
      val closedProducerTransaction = createTransaction(streamID, partition, TransactionStates.Checkpointed, openedProducerTransaction.transactionID)
      val t =
        time(Await.result(
          Future.sequence(Seq(
            client.putProducerState(openedProducerTransaction),
            client.putTransactionData(openedProducerTransaction.stream, openedProducerTransaction.partition, openedProducerTransaction.transactionID, data, (txnCount - 1) * dataSize),
            client.putProducerState(closedProducerTransaction))), 10.seconds)
        )
      (x, t)
    })

    println(s"Write to file $filename")
    writeTransactionsLifeCycleAndTime(filename, result)

    client.shutdown()
  }
}
