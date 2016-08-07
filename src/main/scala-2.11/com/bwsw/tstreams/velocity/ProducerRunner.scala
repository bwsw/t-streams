package com.bwsw.tstreams.velocity

import java.net.InetSocketAddress

import com.bwsw.tstreams.agents.producer.DataInsertType.BatchInsert
import com.bwsw.tstreams.agents.producer.{Producer, Options, CoordinationOptions, NewTransactionProducerPolicy}
import com.bwsw.tstreams.coordination.producer.transport.impl.TcpTransport

object ProducerRunner {
  def main(args: Array[String]) {
    import Common._
    //producer/consumer options
    val agentSettings = new CoordinationOptions(
      agentAddress = "t-streams-2.z1.netpoint-dc.com:8888",
      zkHosts = List(new InetSocketAddress(zkHost, 2181)),
      zkRootPath = "/velocity",
      zkSessionTimeout = 7000,
      isLowPriorityToBeMaster = true,
      transport = new TcpTransport,
      transportTimeout = 5,
      zkConnectionTimeout = 7)

    val producerOptions = new Options[String](transactionTTL = 6, transactionKeepAliveInterval = 2, RoundRobinPolicyCreator.getRoundRobinPolicy(stream, List(0)), BatchInsert(10), LocalGeneratorCreator.getGen(), agentSettings, stringToArrayByteConverter)

    val producer = new Producer[String]("producer", stream, producerOptions)
    var cnt = 0
    var timeNow = System.currentTimeMillis()

    var accNewTxn = 0L
    var accSend = 0L

    while (true) {
      var t1 = System.currentTimeMillis()
      val txn = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
      accNewTxn += (System.currentTimeMillis() - t1)

      t1 = System.currentTimeMillis()
      0 until 10 foreach { x =>
        txn.send(x.toString)
      }
      accSend += (System.currentTimeMillis() - t1)

      txn.checkpoint()
      if (cnt % 1000 == 0) {
        val time = System.currentTimeMillis()
        val diff = time - timeNow
        println(s"producer_time = $diff; cnt=$cnt; accNewTxn=$accNewTxn; accSend=$accSend")
        Delays.printAndClear()
        print("\n\n")
        timeNow = time
        accNewTxn = 0L
        accSend = 0L
      }
      cnt += 1
    }
  }
}
