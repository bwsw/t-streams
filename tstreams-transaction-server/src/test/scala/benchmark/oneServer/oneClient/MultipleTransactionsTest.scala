package benchmark.oneServer.oneClient

import benchmark.utils.Launcher
import benchmark.utils.writer.TransactionMetadataWriter

object MultipleTransactionsTest extends Launcher {
  override val streamName = "stream"
  override val clients = 1
  private val txnCount = 1000000
  private val rand = new scala.util.Random()

  def main(args: Array[String]) {
    launch()
    System.exit(0)
  }

  override def launchClients(streamID: Int) = {
    val filename = rand.nextInt(100) + s"_${txnCount}TransactionMetadataWriterOSOC.csv"
    new TransactionMetadataWriter(streamID).run(txnCount, filename)
  }
}
