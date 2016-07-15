package velocity

import java.net.InetSocketAddress

import com.bwsw.tstreams.agents.producer.InsertionType.BatchInsert
import com.bwsw.tstreams.agents.producer.{BasicProducer, BasicProducerOptions, ProducerCoordinationOptions, ProducerPolicies}
import com.bwsw.tstreams.coordination.transactions.transport.impl.TcpTransport
import testutils.{LocalGeneratorCreator, RoundRobinPolicyCreator}

object ProducerRunner {
  def main(args: Array[String]) {
    import Common._
    //producer/consumer options
    val agentSettings = new ProducerCoordinationOptions(
      agentAddress = "t-streams-2.z1.netpoint-dc.com:8888",
      zkHosts = List(new InetSocketAddress("t-streams-1.z1.netpoint-dc.com", 2181)),
      zkRootPath = "/velocity",
      zkSessionTimeout = 7000,
      isLowPriorityToBeMaster = true,
      transport = new TcpTransport,
      transportTimeout = 5,
      zkConnectionTimeout = 7)

    val producerOptions = new BasicProducerOptions[String, Array[Byte]](
      transactionTTL = 6,
      transactionKeepAliveInterval = 2,
      producerKeepAliveInterval = 1,
      RoundRobinPolicyCreator.getRoundRobinPolicy(stream, (0 until 10).toList),
      BatchInsert(10),
      LocalGeneratorCreator.getGen(),
      agentSettings,
      stringToArrayByteConverter)

    val producer = new BasicProducer[String, Array[Byte]]("producer", stream, producerOptions)
    var cnt = 0
    var timeNow = System.currentTimeMillis()
    while (true) {
      val txn = producer.newTransaction(ProducerPolicies.errorIfOpen)
      0 until 10 foreach { x =>
        txn.send(x.toString)
      }
      txn.checkpoint()
      if (cnt % 1000 == 0) {
        val time = System.currentTimeMillis()
        val diff = time - timeNow
        println(s"producer_time = $diff")
        timeNow = time
      }
      cnt += 1
    }
  }
}
