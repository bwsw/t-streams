package agents.subscriber

import java.io.File
import java.net.InetSocketAddress
import java.util.UUID
import java.util.concurrent.locks.ReentrantLock

import akka.actor.ActorSystem
import com.aerospike.client.Host
import com.bwsw.tstreams.agents.consumer.{BasicConsumerOptions, SubscriberCoordinationOptions}
import com.bwsw.tstreams.agents.consumer.Offsets.Oldest
import com.bwsw.tstreams.agents.consumer.subscriber.{BasicSubscriberCallback, BasicSubscribingConsumer}
import com.bwsw.tstreams.agents.producer.InsertionType.BatchInsert
import com.bwsw.tstreams.agents.producer.{BasicProducer, BasicProducerOptions, ProducerCoordinationOptions, ProducerPolicies}
import com.bwsw.tstreams.converter.{ArrayByteToStringConverter, StringToArrayByteConverter}
import com.bwsw.tstreams.coordination.transactions.transport.impl.TcpTransport
import com.bwsw.tstreams.data.aerospike.{AerospikeStorageFactory, AerospikeStorageOptions}
import com.bwsw.tstreams.debug.GlobalHooks
import com.bwsw.tstreams.metadata.MetadataStorageFactory
import com.bwsw.tstreams.streams.BasicStream
import com.datastax.driver.core.Cluster
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers, OneInstancePerTest}
import testutils.{CassandraHelper, LocalGeneratorCreator, RoundRobinPolicyCreator, TestUtils}

//TODO refactoring
class ABasicSubscriberAfterCommitFailureTest extends FlatSpec with Matchers
  with BeforeAndAfterAll with TestUtils {
  implicit val system = ActorSystem("UTEST")

  System.setProperty("DEBUG", "true")
  GlobalHooks.addHook("AfterCommitFailure", () => throw new RuntimeException)

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

  //aerospike storage instances
  val hosts = List(
    new Host("localhost",3000),
    new Host("localhost",3001),
    new Host("localhost",3002),
    new Host("localhost",3003))
  val aerospikeOptions = new AerospikeStorageOptions("test", hosts)
  val aerospikeInstForProducer = storageFactory.getInstance(aerospikeOptions)
  val aerospikeInstForConsumer = storageFactory.getInstance(aerospikeOptions)

  //metadata storage instances
  val metadataStorageInstForProducer = metadataStorageFactory.getInstance(
    cassandraHosts = List(new InetSocketAddress("localhost", 9042)),
    keyspace = randomKeyspace)
  val metadataStorageInstForConsumer = metadataStorageFactory.getInstance(
    cassandraHosts = List(new InetSocketAddress("localhost", 9042)),
    keyspace = randomKeyspace)

  //stream instances for producer/consumer
  val streamForProducer: BasicStream[Array[Byte]] = new BasicStream[Array[Byte]](
    name = "test_stream",
    partitions = 3,
    metadataStorage = metadataStorageInstForProducer,
    dataStorage = aerospikeInstForProducer,
    ttl = 60 * 10,
    description = "some_description")

  val streamForConsumer = new BasicStream[Array[Byte]](
    name = "test_stream",
    partitions = 3,
    metadataStorage = metadataStorageInstForConsumer,
    dataStorage = aerospikeInstForConsumer,
    ttl = 60 * 10,
    description = "some_description")

  val agentSettings = new ProducerCoordinationOptions(
    agentAddress = s"localhost:8000",
    zkHosts = List(new InetSocketAddress("localhost", 2181)),
    zkRootPath = "/unit",
    zkSessionTimeout = 7000,
    isLowPriorityToBeMaster = false,
    transport = new TcpTransport,
    transportTimeout = 5,
    zkConnectionTimeout = 7)

  //producer/consumer options
  val producerOptions = new BasicProducerOptions[String, Array[Byte]](
    transactionTTL = 6,
    transactionKeepAliveInterval = 2,
    producerKeepAliveInterval = 1,
    RoundRobinPolicyCreator.getRoundRobinPolicy(streamForProducer, List(0,1,2)),
    BatchInsert(5),
    LocalGeneratorCreator.getGen(),
    agentSettings,
    stringToArrayByteConverter)

  val consumerOptions = new BasicConsumerOptions[Array[Byte], String](
    transactionsPreload = 10,
    dataPreload = 7,
    consumerKeepAliveInterval = 5,
    arrayByteToStringConverter,
    RoundRobinPolicyCreator.getRoundRobinPolicy(streamForConsumer, List(0,1,2)),
    Oldest,
    LocalGeneratorCreator.getGen(),
    useLastOffset = true)


  val lock = new ReentrantLock()
  var acc = 0
  val producer = new BasicProducer("test_producer", streamForProducer, producerOptions)
  val callback = new BasicSubscriberCallback[Array[Byte], String] {
    override def onEvent(subscriber : BasicSubscribingConsumer[Array[Byte], String], partition: Int, transactionUuid: UUID): Unit = {
      lock.lock()
      acc += 1
      subscriber.setLocalOffset(partition,transactionUuid)
      subscriber.checkpoint()
      lock.unlock()
    }
    override val pollingFrequency: Int = 100
  }
  val path = randomString

  "subscribe consumer" should "retrieve all sent messages" in {
    val totalMsg = 30
    val dataInTxn = 10
    val data = randomString

    var subscribeConsumer = new BasicSubscribingConsumer[Array[Byte],String](
      "test_consumer",
      streamForConsumer,
      consumerOptions,
      new SubscriberCoordinationOptions("localhost:8588", "/unit", List(new InetSocketAddress("localhost",2181)), 7000, 7000),
      callback,
      path)
    subscribeConsumer.start()

    sendTxnsAndWait(totalMsg, dataInTxn, data)
    sendTxnsAndWait(totalMsg, dataInTxn, data)

    subscribeConsumer.stop()

    subscribeConsumer = new BasicSubscribingConsumer[Array[Byte],String](
      "test_consumer",
      new BasicStream[Array[Byte]](
        name = "test_stream",
        partitions = 3,
        metadataStorage = metadataStorageInstForConsumer,
        dataStorage = storageFactory.getInstance(aerospikeOptions),
        ttl = 60 * 10,
        description = "some_description"),
      consumerOptions,
      new SubscriberCoordinationOptions("localhost:8588", "/unit", List(new InetSocketAddress("localhost",2181)), 7000,7000),
      callback,
      path)
    subscribeConsumer.start()

    Thread.sleep(10000)

    subscribeConsumer.stop()

    acc shouldEqual totalMsg * 2
  }

  def sendTxnsAndWait(totalMsg : Int, dataInTxn : Int, data : String) = {
    (0 until totalMsg) foreach { x=>
      val txn = producer.newTransaction(ProducerPolicies.errorIfOpened)
      (0 until dataInTxn) foreach { _ =>
        txn.send(data)
      }
      try {
        txn.checkpoint()
      } catch {
        case e : RuntimeException =>
      }
    }
    Thread.sleep(10000)
  }

  override def afterAll(): Unit = {
    System.clearProperty("DEBUG")
    GlobalHooks.clear()
    producer.stop()
    removeZkMetadata("/unit")
    session.execute(s"DROP KEYSPACE $randomKeyspace")
    session.close()
    cluster.close()
    metadataStorageFactory.closeFactory()
    storageFactory.closeFactory()
    val file = new File(path)
    remove(file)
  }
}
