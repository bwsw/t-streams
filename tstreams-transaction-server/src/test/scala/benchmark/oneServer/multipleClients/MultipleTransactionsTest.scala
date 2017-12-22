package benchmark.oneServer.multipleClients

import benchmark.utils.Launcher
import benchmark.utils.writer.TransactionMetadataWriter

import scala.collection.mutable.ArrayBuffer

object MultipleTransactionsTest extends Launcher {
  override val streamName = "stream"
  override val clients = 1
  private val txnCount = 1000000
  private val rand = new scala.util.Random()
  private val clientThreads = ArrayBuffer[Thread]()

  def main(args: Array[String]) {
    launch()
    System.exit(0)
  }

  override def launchClients(streamID: Int): Unit = {
    (1 to clients).foreach(x => {
      val thread = new Thread(() => {
        val filename = rand.nextInt(100) + s"_${txnCount}TransactionMetadataWriterOSMC.csv"
        new TransactionMetadataWriter(streamID, x).run(txnCount, filename)
      })
      clientThreads.+=(thread)
    })
    clientThreads.foreach(_.start())
    clientThreads.foreach(_.join())
  }
}