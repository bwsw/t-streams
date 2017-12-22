package benchmark.oneServer.oneClient

import benchmark.utils.Launcher
import benchmark.utils.writer.TransactionDataWriter

object OneTransactionTest extends Launcher {
  override val streamName = "stream"
  override val clients = 1
  private val txnCount = 1000000
  private val dataSize = 1
  private val rand = new scala.util.Random()

  def main(args: Array[String]) {
    launch()
    System.exit(0)
  }

  override def launchClients(streamID: Int): Unit = {
    val filename = rand.nextInt(100) + s"TransactionDataWriterTo1PartitionOSOC.csv"
    new TransactionDataWriter(streamID).run(txnCount, dataSize, filename)
  }
}