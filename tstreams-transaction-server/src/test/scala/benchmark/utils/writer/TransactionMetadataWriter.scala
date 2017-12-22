package benchmark.utils.writer

import benchmark.utils.{CsvWriter, TimeMeasure, TransactionCreator}
import benchmark.Options._
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates

import scala.concurrent.Await
import scala.concurrent.duration._

class TransactionMetadataWriter(streamID: Int, partition: Int = 1) extends TransactionCreator with CsvWriter with TimeMeasure {
  def run(txnCount: Int, filename: String) {

    val client = clientBuilder
      .build()

    var globalProgress = 1
    val result = (1 to txnCount).map(x => {
      val localProgress = (x.toDouble / txnCount * 100).round
      if (globalProgress == localProgress) {
        println(localProgress + "%")
        globalProgress += 1
      }

      val openedProducerTransaction = createTransaction(streamID, partition, TransactionStates.Opened)
      (x, {
        time(Await.result(client.putProducerState(openedProducerTransaction), 5.seconds))
      })
    })

    println(s"Write to file $filename")
    writeMetadataTransactionsAndTime(filename, result)

    client.shutdown()
  }
}
