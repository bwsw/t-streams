package agents.both.batch_insert.aerospike

import java.net.InetSocketAddress

import com.bwsw.tstreams.agents.consumer.Offsets.Oldest
import com.bwsw.tstreams.agents.consumer.{BasicConsumer, BasicConsumerOptions}
import com.bwsw.tstreams.agents.producer.InsertionType.BatchInsert
import com.bwsw.tstreams.agents.producer.{BasicProducer, BasicProducerOptions, ProducerCoordinationOptions, ProducerPolicies}
import com.bwsw.tstreams.coordination.transactions.transport.impl.TcpTransport
import com.bwsw.tstreams.streams.BasicStream
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._


class AManyBasicProducersStreamingInManyPartitionsAndConsumerWithCheckpointsTest extends FlatSpec
  with Matchers with BeforeAndAfterAll with TestUtils {
  var port = 8000

  "Some amount of producers and one consumer" should "producers - send transactions in many partition" +
    " (each producer send each txn in only one partition without intersection " +
    " for ex. producer1 in partition1, producer2 in partition2, producer3 in partition3 etc...) " +
    " consumer - retrieve them all with reinitialization every 30 transactions" in {
    val timeoutForWaiting = 60 * 5
    val totalPartitions = 4
    val totalTxn = 10
    val totalElementsInTxn = 3
    val producersAmount = 10
    val dataToSend = (for (part <- 0 until totalElementsInTxn) yield randomString).sorted

    val producers: List[BasicProducer[String]] =
      (0 until producersAmount)
        .toList
        .map(x => getProducer(List(x % totalPartitions), totalPartitions))

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

    val streamInst = getStream(totalPartitions)

    val consumerOptions = new BasicConsumerOptions[String](transactionsPreload = 10, dataPreload = 7, arrayByteToStringConverter, RoundRobinPolicyCreator.getRoundRobinPolicy(
            usedPartitions = (0 until totalPartitions).toList,
            stream = streamInst), Oldest, LocalGeneratorCreator.getGen(), useLastOffset = true)

    var checkVal = true

    var consumer = new BasicConsumer("test_consumer", streamInst, consumerOptions)
    consumer.start

    val consumerThread = new Thread(
      new Runnable {
        Thread.sleep(3000)

        def run() = {
          var i = 0
          while (i < totalTxn * producersAmount) {

            if (i % 30 == 0) {
              consumer.checkpoint()

              val newStreamForConsumer = new BasicStream[Array[Byte]](
                name = "stream_name",
                partitions = totalPartitions,
                metadataStorage = streamInst.metadataStorage,
                dataStorage = storageFactory.getInstance(aerospikeOptions),
                ttl = 60 * 10,
                description = "some_description")

              consumer = new BasicConsumer("test_consumer", newStreamForConsumer, consumerOptions)
              consumer.start
              Thread.sleep(1000)
            }

            val txn = consumer.getTransaction

            if (txn.isDefined) {
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
    (0 until totalPartitions) foreach { _ =>
      checkVal &= consumer.getTransaction.isEmpty
    }

    checkVal &= !consumerThread.isAlive
    producersThreads.foreach(x => checkVal &= !x.isAlive)

    producers.foreach(_.stop())

    checkVal shouldEqual true
  }

  def getProducer(usedPartitions: List[Int], totalPartitions: Int): BasicProducer[String] = {
    val stream = getStream(totalPartitions)

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

    val producerOptions = new BasicProducerOptions[String](transactionTTL = 6, transactionKeepAliveInterval = 2, writePolicy = RoundRobinPolicyCreator.getRoundRobinPolicy(stream, usedPartitions), BatchInsert(batchSizeTestVal), LocalGeneratorCreator.getGen(), agentSettings, converter = stringToArrayByteConverter)

    val producer = new BasicProducer("test_producer1", stream, producerOptions)
    producer
  }

  def getStream(partitions: Int): BasicStream[Array[Byte]] = {
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
    onAfterAll()
  }
}
