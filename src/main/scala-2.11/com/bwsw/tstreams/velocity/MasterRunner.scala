package com.bwsw.tstreams.velocity

import java.net.InetSocketAddress

import com.bwsw.tstreams.agents.producer.InsertionType.BatchInsert
import com.bwsw.tstreams.agents.producer.{BasicProducer, BasicProducerOptions, ProducerCoordinationOptions}
import com.bwsw.tstreams.coordination.transactions.transport.impl.TcpTransport

object MasterRunner {
  import Common._
  def main(args: Array[String]) {
    //producer/consumer options
    val agentSettings = new ProducerCoordinationOptions(
      agentAddress = "t-streams-3.z1.netpoint-dc.com:8888",
      zkHosts = List(new InetSocketAddress(zkHost, 2181)),
      zkRootPath = "/velocity",
      zkSessionTimeout = 7000,
      isLowPriorityToBeMaster = false,
      transport = new TcpTransport,
      transportTimeout = 5,
      zkConnectionTimeout = 7)

    val producerOptions = new BasicProducerOptions[String](
      transactionTTL = 6,
      transactionKeepAliveInterval = 2,
      producerKeepAliveInterval = 1,
      RoundRobinPolicyCreator.getRoundRobinPolicy(stream, List(0)),
      BatchInsert(10),
      LocalGeneratorCreator.getGen(),
      agentSettings,
      stringToArrayByteConverter)

    new BasicProducer[String]("master", stream, producerOptions)
  }
}