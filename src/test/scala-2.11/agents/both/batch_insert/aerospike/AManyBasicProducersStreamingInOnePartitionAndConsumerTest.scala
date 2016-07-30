package agents.both.batch_insert.aerospike

import java.net.InetSocketAddress
import java.util.UUID

import com.bwsw.tstreams.agents.consumer.Offsets.Oldest
import com.bwsw.tstreams.agents.consumer.{BasicConsumer, BasicConsumerOptions}
import com.bwsw.tstreams.agents.producer.InsertionType.BatchInsert
import com.bwsw.tstreams.agents.producer.{BasicProducer, BasicProducerOptions, ProducerCoordinationOptions, ProducerPolicies}
import com.bwsw.tstreams.coordination.transactions.transport.impl.TcpTransport
import com.bwsw.tstreams.streams.BasicStream
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{LocalGeneratorCreator, RoundRobinPolicyCreator, TestUtils}

import scala.collection.mutable.ListBuffer


class AManyBasicProducersStreamingInOnePartitionAndConsumerTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  var port = 8000

  "Some amount of producers and one consumer" should "producers - send transactions in one partition and consumer - retrieve them all" in {
    val timeoutForWaiting = 60 * 5
    val totalTxn = 10
    val totalElementsInTxn = 10
    val producersAmount = 15
    val dataToSend = (for (part <- 0 until totalElementsInTxn) yield randomString).sorted
    val producers: List[BasicProducer[String]] = (0 until producersAmount).toList.map(x => getProducer)
    val producersThreads = producers.map(p =>
      new Thread(new Runnable {
        def run() {
          var i = 0
          while (i < totalTxn) {
            Thread.sleep(2000)
            val txn = p.newTransaction(ProducerPolicies.errorIfOpened)
            dataToSend.foreach(x => txn.send(x))
            txn.checkpoint()
            i += 1
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
          while (i < totalTxn * producersAmount) {
            val txn = consumer.getTransaction
            if (txn.isDefined) {
              uuids += txn.get.getTxnUUID
              checkVal &= txn.get.getAll().sorted == dataToSend
              i += 1
            }
            Thread.sleep(200)
          }
        }
      })

    producersThreads.foreach(x => x.start())
    consumerThread.start()
    consumerThread.join(timeoutForWaiting * 1000)
    producersThreads.foreach(x => x.join(timeoutForWaiting * 1000))

    //assert that is nothing to read
    checkVal &= consumer.getTransaction.isEmpty
    checkVal &= !consumerThread.isAlive
    producersThreads.foreach(x => checkVal &= !x.isAlive)
    checkVal &= isSorted(uuids)

    producers.foreach(_.stop())

    checkVal shouldEqual true
  }

  def getProducer: BasicProducer[String] = {
    val stream = getStream

    val agentSettings = new ProducerCoordinationOptions(
      agentAddress = s"localhost:$port",
      zkHosts = List(new InetSocketAddress("localhost", 2181)),
      zkRootPath = "/unit",
      zkSessionTimeout = 7000,
      isLowPriorityToBeMaster = false,
      transport = new TcpTransport,
      transportTimeout = 5,
      zkConnectionTimeout = 7)

    port += 1

    val producerOptions = new BasicProducerOptions[String](transactionTTL = 6, transactionKeepAliveInterval = 2, writePolicy = RoundRobinPolicyCreator.getRoundRobinPolicy(stream, List(0)), BatchInsert(batchSizeTestVal), LocalGeneratorCreator.getGen(), agentSettings, converter = stringToArrayByteConverter)

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
    onAfterAll()
  }
}