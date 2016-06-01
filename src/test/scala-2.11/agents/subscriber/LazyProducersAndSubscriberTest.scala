package agents.subscriber


import java.io.File
import java.net.InetSocketAddress
import java.util.UUID
import java.util.concurrent.locks.ReentrantLock
import com.aerospike.client.Host
import com.bwsw.tstreams.agents.consumer.Offsets.Oldest
import com.bwsw.tstreams.agents.consumer.subscriber.{BasicSubscriberCallback, BasicSubscribingConsumer}
import com.bwsw.tstreams.agents.consumer.{ConsumerCoordinationOptions, BasicConsumerOptions}
import com.bwsw.tstreams.agents.producer.InsertionType.BatchInsert
import com.bwsw.tstreams.agents.producer.{ProducerCoordinationOptions, BasicProducer, BasicProducerOptions, ProducerPolicies}
import com.bwsw.tstreams.converter.{ArrayByteToStringConverter, StringToArrayByteConverter}
import com.bwsw.tstreams.data.aerospike.{AerospikeStorageFactory, AerospikeStorageOptions}
import com.bwsw.tstreams.coordination.transactions.transport.impl.TcpTransport
import com.bwsw.tstreams.metadata.MetadataStorageFactory
import com.bwsw.tstreams.streams.BasicStream
import com.datastax.driver.core.Cluster
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._
import scala.collection.mutable.ListBuffer

class LazyProducersAndSubscriberTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils{
  var port = 8000

  //creating keyspace, metadata
  val path = randomString
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

  "Some amount of producers and subscriber" should "producers - send transactions in many partition" +
    " (each producer send each txn in only one partition without intersection " +
    " for ex. producer1 in partition1, producer2 in partition2, producer3 in partition3 etc...)," +
    " subscriber - retrieve them all(with callback) in sorted order" in {

    val timeoutForWaiting = 60
    val totalPartitions = 4
    val totalTxn = 10
    val totalElementsInTxn = 3
    val producersAmount = 10
    val dataToSend = (for (part <- 0 until totalElementsInTxn) yield randomString).sorted

    val producers: List[BasicProducer[String, Array[Byte]]] =
      (0 until producersAmount)
        .toList
        .map(x=>getProducer(List(x%totalPartitions),totalPartitions))

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

    val streamInst = getStream(totalPartitions)

    val consumerOptions = new BasicConsumerOptions[Array[Byte], String](
      transactionsPreload = 10,
      dataPreload = 7,
      consumerKeepAliveInterval = 5,
      arrayByteToStringConverter,
      RoundRobinPolicyCreator.getRoundRobinPolicy(
        usedPartitions = (0 until totalPartitions).toList,
        stream = streamInst),
      new ConsumerCoordinationOptions("localhost:8588", "/unit", List(new InetSocketAddress("localhost",2181)), 7000),
      Oldest,
      LocalGeneratorCreator.getGen(),
      useLastOffset = false)

    val lock = new ReentrantLock()
    val map = scala.collection.mutable.Map[Int,ListBuffer[UUID]]()
    (0 until streamInst.getPartitions) foreach { partition =>
      map(partition) = ListBuffer.empty[UUID]
    }

    var cnt = 0

    val callback = new BasicSubscriberCallback[Array[Byte], String] {
      override def onEvent(subscriber : BasicSubscribingConsumer[Array[Byte], String], partition: Int, transactionUuid: UUID): Unit = {
        lock.lock()
        cnt += 1
        map(partition) += transactionUuid
        lock.unlock()
      }
      override val pollingFrequency: Int = 100
    }
    val subscriber = new BasicSubscribingConsumer("test_consumer", streamInst, consumerOptions, callback, path)

    producersThreads.foreach(x=>x.start())
    Thread.sleep(10000)
    subscriber.start()
    producersThreads.foreach(x=>x.join(timeoutForWaiting*1000L))
    Thread.sleep(30*1000)

    producers.foreach(_.stop())
    subscriber.stop()
    assert(map.values.map(x=>x.size).sum == totalTxn*producersAmount)
    map foreach {case(_,list) =>
      list.map(x=>(x,x.timestamp())).sortBy(_._2).map(x=>x._1) shouldEqual list
    }
  }

  def getProducer(usedPartitions : List[Int], totalPartitions : Int) : BasicProducer[String,Array[Byte]] = {
    val stream = getStream(totalPartitions)

    val agentSettings = new ProducerCoordinationOptions(
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
      writePolicy = RoundRobinPolicyCreator.getRoundRobinPolicy(stream, usedPartitions),
      BatchInsert(batchSizeVal),
      LocalGeneratorCreator.getGen(),
      agentSettings,
      converter = stringToArrayByteConverter)

    val producer = new BasicProducer("test_producer1", stream, producerOptions)
    producer
  }

  def getStream(partitions : Int): BasicStream[Array[Byte]] = {
    //storage instances
    val metadataStorageInst = metadataStorageFactory.getInstance(
      cassandraHosts = List(new InetSocketAddress("localhost", 9042)),
      keyspace = randomKeyspace)
    val dataStorageInst = storageFactory.getInstance(aerospikeOptions)

    new BasicStream[Array[Byte]](
      name = "stream_name",
      partitions = partitions,
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
    val file = new File(path)
    remove(file)
  }

  def remove(f : File) : Unit = {
    if (f.isDirectory) {
      for (c <- f.listFiles())
        remove(c)
    }
    f.delete()
  }
}