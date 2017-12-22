package benchmark.oneServer.multipleClients

import benchmark.utils.Launcher
import benchmark.utils.writer.TransactionLifeCycleWriter

import scala.collection.mutable.ArrayBuffer

object MultipleTransactionLifeCyclesTest
  extends Launcher {

  override val clients = 4
  override val streamName = "stream"
  private val txnCount = 1000000
  private val dataSize = 1
  private val clientThreads = ArrayBuffer[Thread]()
  private val rand = new scala.util.Random()

  def main(args: Array[String]) {
   // launch()
    val streamID = createStream(streamName, clients)
    launchClients(streamID)
    System.exit(0)
  }

  override def launchClients(streamID: Int): Unit = {
    (1 to clients).foreach(x => {
      val thread = new Thread(() => {
        val filename = rand.nextInt(100) + s"_${txnCount}TransactionLifeCycleWriterOSMC.csv"
        new TransactionLifeCycleWriter(streamID, x).run(txnCount, dataSize, filename)
      })
      clientThreads.+=(thread)
    })
    clientThreads.foreach(_.start())
    clientThreads.foreach(_.join())
  }
}
