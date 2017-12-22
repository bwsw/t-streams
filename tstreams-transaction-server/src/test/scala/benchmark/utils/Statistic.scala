package benchmark.utils

import scala.io.Source

object Statistic {
  def mean(items: Traversable[Int]): Double = {
    items.sum.toDouble / items.size
  }

  def variance(items: Traversable[Int]): Double = {
    val itemMean = mean(items)
    val count = items.size
    val sumOfSquares = items.foldLeft(0.0d)((total, item) => {
      val square = math.pow(item - itemMean, 2)
      total + square
    })
    sumOfSquares / count.toDouble
  }

  def stddev(items: Traversable[Int]): Double = {
    math.sqrt(variance(items))
  }
}


object CI extends App {
  val lines = Source.fromFile("49_1000000TransactionLifeCycleWriterOSMC.csv").getLines
  val time = lines.drop(1).map(x => x.split(",")(1).trim.toInt).toTraversable
  val mean = Statistic.mean(time)
  val stddev = Statistic.stddev(time)

  println("Mean: " + mean)
  println("Confidence interval: (" + (mean - 1.960 * stddev) + ", " + (mean + 1.960 * stddev) + ")")
}