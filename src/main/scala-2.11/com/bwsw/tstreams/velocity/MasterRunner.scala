package com.bwsw.tstreams.velocity

import java.net.InetSocketAddress

import com.bwsw.tstreams.agents.producer.DataInsertType.BatchInsert
import com.bwsw.tstreams.agents.producer.{Producer, Options, CoordinationOptions}
import com.bwsw.tstreams.coordination.producer.transport.impl.TcpTransport

object MasterRunner {

  import Common._

  def main(args: Array[String]) {
    //producer/consumer options
    val agentSettings = new CoordinationOptions(
      agentAddress = "t-streams-3.z1.netpoint-dc.com:8888",
      zkHosts = List(new InetSocketAddress(zkHost, 2181)),
      zkRootPath = "/velocity",
      zkSessionTimeout = 7000,
      isLowPriorityToBeMaster = false,
      transport = new TcpTransport,
      transportTimeout = 5,
      zkConnectionTimeout = 7)

    val producerOptions = new Options[String](transactionTTL = 6, transactionKeepAliveInterval = 2, RoundRobinPolicyCreator.getRoundRobinPolicy(stream, List(0)), BatchInsert(10), LocalGeneratorCreator.getGen(), agentSettings, stringToArrayByteConverter)

    new Producer[String]("master", stream, producerOptions)
  }
}