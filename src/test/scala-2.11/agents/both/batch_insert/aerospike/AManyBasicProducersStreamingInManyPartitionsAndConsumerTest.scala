package agents.both.batch_insert.aerospike

import com.bwsw.tstreams.agents.consumer.Offsets.Oldest
import com.bwsw.tstreams.agents.producer.{BasicProducer, ProducerPolicies}
import com.bwsw.tstreams.env.TSF_Dictionary
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._



class AManyBasicProducersStreamingInManyPartitionsAndConsumerTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  var port = 8000
  val timeoutForWaiting = 60 * 5
  val totalPartitions = 4
  val totalTxn = 10
  val totalElementsInTxn = 3
  val producersAmount = 10
  val dataToSend = (for (part <- 0 until totalElementsInTxn) yield randomString).sorted

  f.setProperty(TSF_Dictionary.Stream.name, "test_stream").
    setProperty(TSF_Dictionary.Stream.partitions, totalPartitions).
    setProperty(TSF_Dictionary.Stream.ttl, 60 * 10).
    setProperty(TSF_Dictionary.Coordination.connection_timeout, 7).
    setProperty(TSF_Dictionary.Coordination.ttl, 7).
    setProperty(TSF_Dictionary.Producer.master_timeout, 5).
    setProperty(TSF_Dictionary.Producer.Transaction.ttl, 6).
    setProperty(TSF_Dictionary.Producer.Transaction.keep_alive, 2).
    setProperty(TSF_Dictionary.Consumer.transaction_preload, 10).
    setProperty(TSF_Dictionary.Consumer.data_preload, 10)

  val producers: List[BasicProducer[String]] =
    (0 until producersAmount)
      .toList
      .map(x => getProducer(List(x % totalPartitions), totalPartitions))

  val producersThreads =
    producers.map(p =>
    new Thread(new Runnable {
      def run() {
        var i = 0
        while (i < totalTxn) {
          //Thread.sleep(2000)
          val txn = p.newTransaction(ProducerPolicies.errorIfOpened)
          dataToSend.foreach(x => txn.send(x))
          txn.checkpoint()
          i += 1
        }
      }
    }))


  var checkVal = true

  val consumer = f.getConsumer[String](
    name = "test_consumer",
    txnGenerator = LocalGeneratorCreator.getGen(),
    converter = arrayByteToStringConverter,
    partitions = (0 until totalPartitions).toList,
    offset = Oldest,
    isUseLastOffset = false)

  consumer.start


  "Some amount of producers and one consumer" should "producers - send transactions in many partition" +
    " (each producer send each txn in only one partition without intersection " +
    " for ex. producer1 in partition1, producer2 in partition2, producer3 in partition3 etc...)," +
    " consumer - retrieve them all" in {

    val consumerThread = new Thread(
      new Runnable {
        def run() = {
          var i = 0
          while (i < totalTxn * producersAmount) {
            logger.info(s"I: ${i} / Total is: ${ totalTxn * producersAmount }")
            val txn = consumer.getTransaction
            if (txn.isDefined) {
              checkVal &= txn.get.getAll().sorted == dataToSend
              i += 1
            }
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
//    val stream = getStream(totalPartitions)
//
//    val agentSettings = new ProducerCoordinationOptions(
//      agentAddress = s"localhost:$port",
//      zkHosts = List(new InetSocketAddress("localhost", 2181)),
//      zkRootPath = "/unit",
//      zkSessionTimeout = 7000,
//      isLowPriorityToBeMaster = false,
//      transport = new TcpTransport,
//      transportTimeout = 5,
//      zkConnectionTimeout = 7)
//
//    port += 1
//
//    val producerOptions = new BasicProducerOptions[String](
//      transactionTTL = 6,
//      transactionKeepAliveInterval = 2,
//      writePolicy = RoundRobinPolicyCreator.getRoundRobinPolicy(stream, usedPartitions),
//      BatchInsert(batchSizeTestVal),
//      LocalGeneratorCreator.getGen(),
//      agentSettings,
//      converter = stringToArrayByteConverter)
//
//    val producer = new BasicProducer("test_producer1", stream, producerOptions)
//    producer

    val port = TestUtils.getPort
    f.setProperty(TSF_Dictionary.Producer.master_bind_port, port)
    f.getProducer[String](
      name = "test_producer",
      txnGenerator = LocalGeneratorCreator.getGen(),
      converter = stringToArrayByteConverter,
      partitions = usedPartitions,
      isLowPriority = false)
  }


  override def afterAll(): Unit = {
    onAfterAll()
  }
}