package com.bwsw.tstreams.velocity

import java.net.InetSocketAddress
import java.util.UUID
import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreams.agents.consumer.{BasicConsumerOptions, SubscriberCoordinationOptions}
import com.bwsw.tstreams.agents.consumer.Offsets.Oldest
import com.bwsw.tstreams.agents.consumer.subscriber.{BasicSubscriberCallback, BasicSubscribingConsumer}
import testutils.{LocalGeneratorCreator, RoundRobinPolicyCreator}


object SubscriberRunner {
  def main(args: Array[String]) {
    import Common._
    val consumerOptions = new BasicConsumerOptions[Array[Byte], String](
      transactionsPreload = 10,
      dataPreload = 7,
      consumerKeepAliveInterval = 5,
      arrayByteToStringConverter,
      RoundRobinPolicyCreator.getRoundRobinPolicy(stream, List(0)),
      Oldest,
      LocalGeneratorCreator.getGen(),
      useLastOffset = true)

    val lock = new ReentrantLock()
    var cnt = 0
    var timeNow = System.currentTimeMillis()
    val callback = new BasicSubscriberCallback[Array[Byte], String] {
      override def onEvent(subscriber: BasicSubscribingConsumer[Array[Byte], String], partition: Int, transactionUuid: UUID): Unit = {
        lock.lock()
        if (cnt % 1000 == 0){
          val time = System.currentTimeMillis()
          val diff = time - timeNow
          println(s"subscriber_time = $diff; cnt=$cnt")
          timeNow = time
        }
        cnt += 1
        lock.unlock()
      }
      override val pollingFrequency: Int = 100
    }

    val subscribeConsumer = new BasicSubscribingConsumer[Array[Byte], String](
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
