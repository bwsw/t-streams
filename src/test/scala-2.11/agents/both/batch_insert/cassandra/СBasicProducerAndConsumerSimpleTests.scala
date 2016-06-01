package agents.both.batch_insert.cassandra

import java.net.InetSocketAddress
import com.bwsw.tstreams.agents.consumer.Offsets.Oldest
import com.bwsw.tstreams.agents.consumer.{SubscriberCoordinationOptions, BasicConsumer, BasicConsumerOptions, BasicConsumerTransaction}
import com.bwsw.tstreams.agents.producer.InsertionType.BatchInsert
import com.bwsw.tstreams.agents.producer.{ProducerCoordinationOptions, BasicProducer, BasicProducerOptions, ProducerPolicies}
import com.bwsw.tstreams.converter.{ArrayByteToStringConverter, StringToArrayByteConverter}
import com.bwsw.tstreams.data.cassandra.{CassandraStorageFactory, CassandraStorageOptions}
import com.bwsw.tstreams.coordination.transactions.transport.impl.TcpTransport
import com.bwsw.tstreams.metadata.MetadataStorageFactory
import com.bwsw.tstreams.streams.BasicStream
import com.datastax.driver.core.Cluster
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._


class СBasicProducerAndConsumerSimpleTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils{
  //creating keyspace, metadata
  val randomKeyspace = randomString
  val cluster = Cluster.builder().addContactPoint("localhost").build()
  val session = cluster.connect()
  CassandraHelper.createKeyspace(session, randomKeyspace)
  CassandraHelper.createMetadataTables(session, randomKeyspace)
  CassandraHelper.createDataTable(session, randomKeyspace)

  //metadata/data factories
  val metadataStorageFactory = new MetadataStorageFactory
  val storageFactory = new CassandraStorageFactory

  //converters to convert usertype->storagetype; storagetype->usertype
  val arrayByteToStringConverter = new ArrayByteToStringConverter
  val stringToArrayByteConverter = new StringToArrayByteConverter

  //cassandra storage instances
  val cassandraStorageOptions = new CassandraStorageOptions(List(new InetSocketAddress("localhost",9042)), randomKeyspace)
  val cassandraInstForProducer = storageFactory.getInstance(cassandraStorageOptions)
  val cassandraInstForConsumer = storageFactory.getInstance(cassandraStorageOptions)

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
    dataStorage = cassandraInstForProducer,
    ttl = 60 * 10,
    description = "some_description")

  val streamForConsumer = new BasicStream[Array[Byte]](
    name = "test_stream",
    partitions = 3,
    metadataStorage = metadataStorageInstForConsumer,
    dataStorage = cassandraInstForConsumer,
    ttl = 60 * 10,
    description = "some_description")

  val agentSettings = new ProducerCoordinationOptions(
    agentAddress = "localhost:8888",
    zkHosts = List(new InetSocketAddress("localhost", 2181)),
    zkRootPath = "/unit",
    zkTimeout = 7000,
    isLowPriorityToBeMaster = false,
    transport = new TcpTransport,
    transportTimeout = 5)

  //producer/consumer options
  val producerOptions = new BasicProducerOptions[String, Array[Byte]](
    transactionTTL = 6,
    transactionKeepAliveInterval = 2,
    producerKeepAliveInterval = 1,
    RoundRobinPolicyCreator.getRoundRobinPolicy(streamForProducer, List(0,1,2)),
    BatchInsert(batchSizeTestVal),
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

  val producer = new BasicProducer("test_producer", streamForProducer, producerOptions)
  val consumer = new BasicConsumer("test_consumer", streamForConsumer, consumerOptions)

  "producer, consumer" should "producer - generate one transaction, consumer - retrieve it with getAll method" in {
    CassandraHelper.clearMetadataTables(session, randomKeyspace)
    val totalDataInTxn = 10
    val producerTransaction = producer.newTransaction(ProducerPolicies.errorIfOpen)
    val sendData = (for (part <- 0 until totalDataInTxn) yield "data_part_" + randomString).sorted
    sendData.foreach{ x=>
      producerTransaction.send(x)
    }
    producerTransaction.checkpoint()
    val txnOpt = consumer.getTransaction
    val txn = txnOpt.get

    var checkVal = txn.getAll().sorted == sendData

    //assert that is nothing to read
    (0 until consumer.stream.getPartitions) foreach { _=>
      checkVal &= consumer.getTransaction.isEmpty
    }

    checkVal shouldEqual true
  }

  "producer, consumer" should "producer - generate one transaction, consumer - retrieve it using iterator" in {
    CassandraHelper.clearMetadataTables(session, randomKeyspace)
    val totalDataInTxn = 10
    val producerTransaction = producer.newTransaction(ProducerPolicies.errorIfOpen)
    val sendData = (for (part <- 0 until totalDataInTxn) yield "data_part_" + randomString).sorted
    sendData.foreach{ x=>
      producerTransaction.send(x)
    }
    producerTransaction.checkpoint()
    val txnOpt = consumer.getTransaction
    assert(txnOpt.isDefined)
    val txn = txnOpt.get
    var dataToAssert = ListBuffer[String]()
    while(txn.hasNext()){
      dataToAssert += txn.next()
    }

    var checkVal = true

    //assert that is nothing to read
    (0 until consumer.stream.getPartitions) foreach { _ =>
      checkVal &= consumer.getTransaction.isEmpty
    }

    checkVal &= dataToAssert.toList.sorted == sendData

    checkVal shouldEqual true
  }

  "producer, consumer" should "producer - generate some set of transactions, consumer - retrieve them all" in {
    CassandraHelper.clearMetadataTables(session, randomKeyspace)
    val totalTxn = 100
    val totalDataInTxn = 10
    val sendData = (for (part <- 0 until totalDataInTxn) yield "data_part_" + randomString).sorted

    (0 until totalTxn).foreach { _=>
        val producerTransaction = producer.newTransaction(ProducerPolicies.errorIfOpen)
        sendData.foreach{ x=>
          producerTransaction.send(x)
        }
        producerTransaction.checkpoint()
    }

    var checkVal = true

    (0 until totalTxn).foreach { _=>
        val txn = consumer.getTransaction
        checkVal &= txn.nonEmpty
        checkVal &= txn.get.getAll().sorted == sendData
    }

    //assert that is nothing to read
    (0 until consumer.stream.getPartitions) foreach { _ =>
      checkVal &= consumer.getTransaction.isEmpty
    }

    checkVal shouldBe true
  }

  "producer, consumer" should "producer - generate transaction, consumer retrieve it (both start async)" in {
    CassandraHelper.clearMetadataTables(session, randomKeyspace)
    val timeoutForWaiting = 120
    val totalDataInTxn = 10
    val sendData = (for (part <- 0 until totalDataInTxn) yield "data_part_" + part).sorted

    val producerThread = new Thread(new Runnable {
      def run() {
        val txn = producer.newTransaction(ProducerPolicies.errorIfOpen)
        sendData.foreach{ x=>
          txn.send(x)
          Thread.sleep(1000)
        }
        txn.checkpoint()
      }
    })

    var checkVal = true

    val consumerThread = new Thread(new Runnable {
      def run() {
        breakable{while(true) {
          val consumedTxn: Option[BasicConsumerTransaction[Array[Byte], String]] = consumer.getTransaction
          if (consumedTxn.isDefined) {
            checkVal &= consumedTxn.get.getAll().sorted == sendData
            break()
          }
          Thread.sleep(1000)
        }}
      }
    })

    producerThread.start()
    consumerThread.start()
    producerThread.join(timeoutForWaiting*1000)
    consumerThread.join(timeoutForWaiting*1000)
    checkVal &= !producerThread.isAlive
    checkVal &= !consumerThread.isAlive

    //assert that is nothing to read
    (0 until consumer.stream.getPartitions) foreach { _ =>
      checkVal &= consumer.getTransaction.isEmpty
    }

    checkVal shouldEqual true
  }

  override def afterAll(): Unit = {
    producer.stop()
    removeZkMetadata("/unit")
    session.execute(s"DROP KEYSPACE $randomKeyspace")
    session.close()
    cluster.close()
    metadataStorageFactory.closeFactory()
    storageFactory.closeFactory()
  }
}