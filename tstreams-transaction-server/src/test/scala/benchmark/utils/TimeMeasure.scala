package benchmark.utils

trait TimeMeasure {
  def time(block: => Unit) = {
    val t0 = System.currentTimeMillis()
    block
    val t1 = System.currentTimeMillis()
    t1 - t0
  }
}
