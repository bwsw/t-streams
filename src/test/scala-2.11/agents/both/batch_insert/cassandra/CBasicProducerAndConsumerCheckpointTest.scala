package agents.both.batch_insert.cassandra

import java.net.InetSocketAddress

import com.bwsw.tstreams.agents.consumer.Offsets.Oldest
import com.bwsw.tstreams.agents.consumer.{BasicConsumer, BasicConsumerOptions, BasicConsumerTransaction}
import com.bwsw.tstreams.agents.producer.InsertionType.BatchInsert
import com.bwsw.tstreams.agents.producer.{BasicProducer, BasicProducerOptions, ProducerCoordinationOptions, ProducerPolicies}
import com.bwsw.tstreams.coordination.transactions.transport.impl.TcpTransport
import com.bwsw.tstreams.streams.BasicStream
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._


class CBasicProducerAndConsumerCheckpointTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  val cassandraInstForProducer = cassandraStorageFactory.getInstance(cassandraStorageOptions)
  val cassandraInstForConsumer = cassandraStorageFactory.getInstance(cassandraStorageOptions)

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
    zkSessionTimeout = 7000,
    isLowPriorityToBeMaster = false,
    transport = new TcpTransport,
    transportTimeout = 5,
    zkConnectionTimeout = 7)

  //producer/consumer options
  val producerOptions = new BasicProducerOptions[String](transactionTTL = 6, transactionKeepAliveInterval = 2, RoundRobinPolicyCreator.getRoundRobinPolicy(streamForProducer, List(0, 1, 2)), BatchInsert(batchSizeTestVal), LocalGeneratorCreator.getGen(), agentSettings, stringToArrayByteConverter)

  val consumerOptions = new BasicConsumerOptions[String](transactionsPreload = 10, dataPreload = 7, arrayByteToStringConverter, RoundRobinPolicyCreator.getRoundRobinPolicy(streamForConsumer, List(0, 1, 2)), Oldest, LocalGeneratorCreator.getGen(), useLastOffset = true)

  val producer = new BasicProducer("test_producer", streamForProducer, producerOptions)
  var consumer = new BasicConsumer("test_consumer", streamForConsumer, consumerOptions)


  "producer, consumer" should "producer - generate many transactions, consumer - retrieve all of them with reinitialization after some time" in {
    val dataToSend = (for (i <- 0 until 10) yield randomString).sorted
    val txnNum = 20

    (0 until txnNum) foreach { _ =>
      val txn = producer.newTransaction(ProducerPolicies.errorIfOpened)
      dataToSend foreach { part =>
        txn.send(part)
      }
      txn.checkpoint()
    }

    val firstPart = txnNum / 3
    val secondPart = txnNum - firstPart

    var checkVal = true

    (0 until firstPart) foreach { _ =>
      val txn: BasicConsumerTransaction[String] = consumer.getTransaction.get
      val data = txn.getAll().sorted
      consumer.checkpoint()
      checkVal &= data == dataToSend
    }

    val newStreamForConsumer = new BasicStream[Array[Byte]](
      name = "test_stream",
      partitions = 3,
      metadataStorage = metadataStorageInstForConsumer,
      dataStorage = cassandraStorageFactory.getInstance(cassandraStorageOptions),
      ttl = 60 * 10,
      description = "some_description")

    //reinitialization (should begin read from the latest checkpoint)
    consumer = new BasicConsumer("test_consumer", newStreamForConsumer, consumerOptions)

    (0 until secondPart) foreach { _ =>
      val txn: BasicConsumerTransaction[String] = consumer.getTransaction.get
      val data = txn.getAll().sorted
      checkVal &= data == dataToSend
    }

    //assert that is nothing to read
    (0 until streamForConsumer.getPartitions) foreach { _ =>
      checkVal &= consumer.getTransaction.isEmpty
    }

    checkVal shouldBe true
  }

  override def afterAll(): Unit = {
    producer.stop()
    onAfterAll()
  }
}
