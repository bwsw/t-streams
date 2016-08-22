package com.bwsw.tstreams.agents.consumer.subscriber_v2

/**
  * Created by Ivan Kudryavtsev on 22.08.16.
  */
class Poller[T](processingEngine: ProcessingEngine[T], interval: Int) extends Runnable {
  override def run(): Unit = {
    processingEngine.handleQueue(interval)
    processingEngine.getExecutor().submit(this)
  }
}
