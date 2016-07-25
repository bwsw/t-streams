package mastervoting_test_utils

import java.net.InetSocketAddress
import java.util.logging.LogManager

import com.aerospike.client.Host
import com.bwsw.tstreams.agents.producer.InsertionType.BatchInsert
import com.bwsw.tstreams.agents.producer.{BasicProducer, BasicProducerOptions, ProducerCoordinationOptions, ProducerPolicies}
import com.bwsw.tstreams.converter.StringToArrayByteConverter
import com.bwsw.tstreams.data.aerospike.{AerospikeStorageFactory, AerospikeStorageOptions}
import com.bwsw.tstreams.generator.LocalTimeUUIDGenerator
import com.bwsw.tstreams.coordination.transactions.transport.impl.TcpTransport
import com.bwsw.tstreams.metadata.MetadataStorageFactory
import com.bwsw.tstreams.policy.RoundRobinPolicy
import com.bwsw.tstreams.streams.BasicStream


object BasicProducerTest{
  def main(args: Array[String]) {
    LogManager.getLogManager.reset()
//    System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "WARN")
    System.setProperty("org.slf4j.simpleLogger.logFile","testlog.log")
    System.setProperty("org.slf4j.simpleLogger.showDateTime","false")
    System.setProperty("org.slf4j.simpleLogger.log.com.bwsw","DEBUG")

    if (args.length != 9){
      println(s"args size:{${args.length}}")
      args.foreach(println)
      throw new IllegalArgumentException("usage: [cnt] [agentAddress] [zk{host:port}] [cassandra{host:port}]" +
        " [aerospike{host:port}] [redis{host:port}] [delay] [keyspace]" +
        " delay in seconds ; {host:port} separator: / ")
    }
    val cnt = args(0).toInt
    val agentAddress = args(1)
    val zkHosts = args(2).split("/").map{x=>
      val hp = x.split(":")
      val (host,port) = (hp(0),hp(1))
      new InetSocketAddress(host,port.toInt)
    }
    val cassandraHosts = args(3).split("/").map{x=>
      val hp = x.split(":")
      val (host,port) = (hp(0),hp(1))
      new InetSocketAddress(host,port.toInt)
    }
    val aerospikeHosts = args(4).split("/").map{x=>
      val hp = x.split(":")
      val (host,port) = (hp(0),hp(1))
      new Host(host,port.toInt)
    }
    val redisHost = args(5)
    val delay = args(6).toInt

    val agentSettings = new ProducerCoordinationOptions(
      agentAddress = agentAddress,
      zkHosts = zkHosts.toList,
      zkRootPath = "/unit",
      zkSessionTimeout = 7000,
      isLowPriorityToBeMaster = false,
      transport = new TcpTransport,
      transportTimeout = 5,
      zkConnectionTimeout = 7)

    val keyspace = args(7)

    //metadata/data factories
    val metadataStorageFactory = new MetadataStorageFactory
    val storageFactory = new AerospikeStorageFactory

    //converters to convert usertype->storagetype; storagetype->usertype
    val stringToArrayByteConverter = new StringToArrayByteConverter

    //aerospike storage instances
    val aerospikeOptions = new AerospikeStorageOptions("test", aerospikeHosts.toList)
    val aerospikeInstForProducer = storageFactory.getInstance(aerospikeOptions)

    //metadata storage instances
    val metadataStorageInstForProducer = metadataStorageFactory.getInstance(
      cassandraHosts = cassandraHosts.toList,
      keyspace = keyspace)


    //stream instances for producer/consumer
    val streamForProducer: BasicStream[Array[Byte]] = new BasicStream[Array[Byte]](
      name = "test_stream",
      partitions = args(8).toInt,
      metadataStorage = metadataStorageInstForProducer,
      dataStorage = aerospikeInstForProducer,
      ttl = 60 * 10,
      description = "some_description")

    //producer/consumer options
    val producerOptions = new BasicProducerOptions[String](
      transactionTTL = 6,
      transactionKeepAliveInterval = 2,
      producerKeepAliveInterval = 1,
      new RoundRobinPolicy(streamForProducer, (0 until args(8).toInt).toList),
      BatchInsert(10),
      new LocalTimeUUIDGenerator,
      agentSettings,
      stringToArrayByteConverter)

    val producer = new BasicProducer("test_producer", streamForProducer, producerOptions)

    0 until cnt foreach { _ =>
      val txn = producer.newTransaction(ProducerPolicies.errorIfOpened)
      txn.send("info")
      txn.checkpoint()
      Thread.sleep(delay*1000L)
      println(s"txn with uuid:{${txn.getTxnUUID.timestamp()}} was sent")
    }

    producer.master_p2p_agent.stop()
    metadataStorageFactory.closeFactory()
  }
}
