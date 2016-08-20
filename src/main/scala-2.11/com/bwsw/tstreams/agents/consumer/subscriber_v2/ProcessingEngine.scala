package com.bwsw.tstreams.agents.consumer.subscriber_v2

import com.bwsw.tstreams.agents.consumer.Consumer

import scala.collection.mutable

/**
  * Created by ivan on 20.08.16.
  */
class ProcessingEngine[T](consumer: Consumer[T],
                          queue: QueueBuilder.QueueType,
                          callback: Callback[T],
                          threadPoolSize: Int = 1) {
  val lastTransactionsMap = mutable.Map[Int, TransactionState]()



}
