package agents.both.batch_insert.aerospike

import java.net.InetSocketAddress
import java.util.UUID
import com.aerospike.client.Host
import com.bwsw.tstreams.agents.consumer.Offsets.Oldest
import com.bwsw.tstreams.agents.consumer.{ConsumerCoordinationSettings, BasicConsumer, BasicConsumerOptions}
import com.bwsw.tstreams.agents.producer.InsertionType.BatchInsert
import com.bwsw.tstreams.agents.producer.{ProducerCoordinationSettings, ProducerPolicies, BasicProducer, BasicProducerOptions}
import com.bwsw.tstreams.converter.{ArrayByteToStringConverter, StringToArrayByteConverter}
import com.bwsw.tstreams.data.aerospike.{AerospikeStorageFactory, AerospikeStorageOptions}
import com.bwsw.tstreams.coordination.transactions.transport.impl.TcpTransport
import com.bwsw.tstreams.common.zkservice.ZkService
import com.bwsw.tstreams.metadata.MetadataStorageFactory
import com.bwsw.tstreams.streams.BasicStream
import com.datastax.driver.core.Cluster
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{TestUtils, RoundRobinPolicyCreator, LocalGeneratorCreator, CassandraHelper}

import scala.collection.mutable.ListBuffer


class AManyBasicProducersStreamingInOnePartitionAndConsumerTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils{
  var port = 8000

  //creating keyspace, metadata
  val randomKeyspace = randomString
  val cluster = Cluster.builder().addContactPoint("localhost").build()
  val session = cluster.connect()
  CassandraHelper.createKeyspace(session, randomKeyspace)
  CassandraHelper.createMetadataTables(session, randomKeyspace)

  //metadata/data factories
  val metadataStorageFactory = new MetadataStorageFactory
  val storageFactory = new AerospikeStorageFactory

  //converters to convert usertype->storagetype; storagetype->usertype
  val arrayByteToStringConverter = new ArrayByteToStringConverter
  val stringToArrayByteConverter = new StringToArrayByteConverter

  //aerospike storage options
  val hosts = List(
    new Host("localhost",3000),
    new Host("localhost",3001),
    new Host("localhost",3002),
    new Host("localhost",3003))
  val aerospikeOptions = new AerospikeStorageOptions("test", hosts)

  "Some amount of producers and one consumer" should "producers - send transactions in one partition and consumer - retrieve them all" in {
    val timeoutForWaiting = 60*5
    val totalTxn = 10
    val totalElementsInTxn = 10
    val producersAmount = 15
    val dataToSend = (for (part <- 0 until totalElementsInTxn) yield randomString).sorted
    val producers: List[BasicProducer[String, Array[Byte]]] = (0 until producersAmount).toList.map(x=>getProducer)
    val producersThreads = producers.map(p =>
      new Thread(new Runnable {
        def run(){
          var i = 0
          while(i < totalTxn) {
            Thread.sleep(2000)
            val txn = p.newTransaction(ProducerPolicies.errorIfOpen)
            dataToSend.foreach(x => txn.send(x))
            txn.checkpoint()
            i+=1
          }
        }
      }))

    val streamInst = getStream

    val consumerOptions = new BasicConsumerOptions[Array[Byte], String](
      transactionsPreload = 10,
      dataPreload = 7,
      consumerKeepAliveInterval = 5,
      arrayByteToStringConverter,
      RoundRobinPolicyCreator.getRoundRobinPolicy(
        usedPartitions = List(0),
        stream = streamInst),
      new ConsumerCoordinationSettings("localhost:8588", "/unit", List(new InetSocketAddress("localhost",2181)), 7000),
      Oldest,
      LocalGeneratorCreator.getGen(),
      useLastOffset = false)

    var checkVal = true
    val consumer = new BasicConsumer("test_consumer", streamInst, consumerOptions)
    val uuids = new ListBuffer[UUID]()

    val consumerThread = new Thread(
      new Runnable {
      Thread.sleep(3000)
        def run() = {
        var i = 0
        while(i < totalTxn*producersAmount) {
          val txn = consumer.getTransaction
          if (txn.isDefined){
            uuids += txn.get.getTxnUUID
            checkVal &= txn.get.getAll().sorted == dataToSend
            i+=1
          }
          Thread.sleep(200)
        }
      }
    })

    producersThreads.foreach(x=>x.start())
    consumerThread.start()
    consumerThread.join(timeoutForWaiting * 1000)
    producersThreads.foreach(x=>x.join(timeoutForWaiting * 1000))

    //assert that is nothing to read
    checkVal &= consumer.getTransaction.isEmpty
    checkVal &= !consumerThread.isAlive
    producersThreads.foreach(x=> checkVal &= !x.isAlive)
    checkVal &= isSorted(uuids)

    producers.foreach(_.stop())
    consumer.stop()

    checkVal shouldEqual true
  }

  def getProducer: BasicProducer[String,Array[Byte]] = {
    val stream = getStream

    val agentSettings = new ProducerCoordinationSettings(
      agentAddress = s"localhost:$port",
      zkHosts = List(new InetSocketAddress("localhost", 2181)),
      zkRootPath = "/unit",
      zkTimeout = 7000,
      isLowPriorityToBeMaster = false,
      transport = new TcpTransport,
      transportTimeout = 5)

    port += 1

    val producerOptions = new BasicProducerOptions[String, Array[Byte]](
      transactionTTL = 6,
      transactionKeepAliveInterval = 2,
      producerKeepAliveInterval = 1,
      writePolicy = RoundRobinPolicyCreator.getRoundRobinPolicy(stream, List(0)),
      BatchInsert(batchSizeVal),
      LocalGeneratorCreator.getGen(),
      agentSettings,
      converter = stringToArrayByteConverter)

    val producer = new BasicProducer("test_producer1", stream, producerOptions)
    producer
  }

  def getStream: BasicStream[Array[Byte]] = {
    val metadataStorageInst = metadataStorageFactory.getInstance(
      cassandraHosts = List(new InetSocketAddress("localhost", 9042)),
      keyspace = randomKeyspace)
    val dataStorageInst = storageFactory.getInstance(aerospikeOptions)

    new BasicStream[Array[Byte]](
      name = "stream_name",
      partitions = 1,
      metadataStorage = metadataStorageInst,
      dataStorage = dataStorageInst,
      ttl = 60 * 10,
      description = "some_description")
  }

  override def afterAll(): Unit = {
    removeZkMetadata()
    session.execute(s"DROP KEYSPACE $randomKeyspace")
    session.close()
    cluster.close()
    metadataStorageFactory.closeFactory()
    storageFactory.closeFactory()
  }
}