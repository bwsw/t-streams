package com.bwsw.tstreams.velocity

import java.net.InetSocketAddress
import java.util.UUID
import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreams.agents.consumer.Offsets.Oldest
import com.bwsw.tstreams.agents.consumer.subscriber.{Callback, SubscribingConsumer}
import com.bwsw.tstreams.agents.consumer.{ConsumerOptions, SubscriberCoordinationOptions}


object SubscriberRunner {
  def main(args: Array[String]) {
    import Common._
    val consumerOptions = new ConsumerOptions[String](transactionsPreload = 10, dataPreload = 7, arrayByteToStringConverter, RoundRobinPolicyCreator.getRoundRobinPolicy(stream, List(0)), Oldest, LocalGeneratorCreator.getGen(), useLastOffset = true)

    val lock = new ReentrantLock()
    var cnt = 0
    var timeNow = System.currentTimeMillis()
    val callback = new Callback[String] {
      override def onEvent(subscriber: SubscribingConsumer[String], partition: Int, transactionUuid: UUID): Unit = {
        lock.lock()
        if (cnt % 1000 == 0) {
          val time = System.currentTimeMillis()
          val diff = time - timeNow
          println(s"subscriber_time = $diff; cnt=$cnt")
          timeNow = time
        }
        cnt += 1
        lock.unlock()
      }
    }

    val subscribeConsumer = new SubscribingConsumer[String](
      name = "test_consumer",
      stream = stream,
      options = consumerOptions,
      subscriberCoordinationOptions =
        new SubscriberCoordinationOptions(agentAddress = "t-streams-4.z1.netpoint-dc.com:8588",
          zkRootPath = "/velocity",
          zkHosts = List(new InetSocketAddress(zkHost, 2181)),
          zkSessionTimeout = 7,
          zkConnectionTimeout = 7),
      callBack = callback,
      persistentQueuePath = "persistent_queue_path")
    subscribeConsumer.start()
  }
}
