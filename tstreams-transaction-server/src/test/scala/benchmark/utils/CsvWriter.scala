package benchmark.utils

import java.io.{File, PrintWriter}

trait CsvWriter {
  def writeMetadataTransactionsAndTime(filename: String, data: IndexedSeq[(Int, Long)]) = {
    val file = new File(filename)
    val bw = new PrintWriter(file)
    bw.write("Number of transaction, Time (ms)\n")
    data.foreach(x => bw.write(x._1 + ", " + x._2 + "\n"))
    bw.close()
  }

  def writeDataTransactionsAndTime(filename: String, data: IndexedSeq[(Int, Long)]) = {
    val file = new File(filename)
    val bw = new PrintWriter(file)
    bw.write("Number of transaction, Time (ms)\n")
    data.foreach(x => bw.write(x._1 + ", " + x._2 + "\n"))
    bw.close()
  }

  def writeTransactionsLifeCycleAndTime(filename: String, data: IndexedSeq[(Int, Long)]) = {
    val file = new File(filename)
    val bw = new PrintWriter(file)
    bw.write("Number of transaction life cycle, Time (ms)\n")
    data.foreach(x => bw.write(x._1 + ", " + x._2 + "\n"))
    bw.close()
  }
}
