package benchmark.oneServer.multipleClients

import benchmark.utils.Launcher
import benchmark.utils.writer.TransactionDataWriter

import scala.collection.mutable._

object OneTransactionTest extends Launcher {
  override val streamName = "stream"
  override val clients = 2
  private val txnCount = 100000
  private val dataSize = 1000
  private val clientThreads = ArrayBuffer[Thread]()
  private val rand = new scala.util.Random()

  def main(args: Array[String]) {
    launch()
    System.exit(0)
  }

  override def launchClients(streamID: Int): Unit = {
    (1 to clients).foreach(x => {
      val thread = new Thread(() => {
        val filename = rand.nextInt(100) + s"TransactionDataWriterTo${x}PartitionOSMC.csv"
        new TransactionDataWriter(streamID, x).run(txnCount, dataSize, filename)
      })
      clientThreads.+=(thread)
    })
    clientThreads.foreach(_.start())
    clientThreads.foreach(_.join())
  }
}