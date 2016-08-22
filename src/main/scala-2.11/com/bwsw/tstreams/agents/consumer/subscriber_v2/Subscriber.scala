package com.bwsw.tstreams.agents.consumer.subscriber_v2

import com.bwsw.tstreams.streams.TStream

/**
  * Created by Ivan Kudryavtsev on 19.08.16.
  * Class implements subscriber
  */
class Subscriber[T](val name: String,
                    val stream: TStream[Array[Byte]],
                    val options: Options[T],
                    val callback: Callback[T]) {

  val consumer = new com.bwsw.tstreams.agents.consumer.Consumer[T](
      name,
      stream,
      options.asInstanceOf[com.bwsw.tstreams.agents.consumer.Options[T]])


  def start() = {
    consumer.start()
  }
}