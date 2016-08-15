package agents.both.batch_insert.aerospike


import com.bwsw.tstreams.agents.consumer.Offsets.Oldest
import com.bwsw.tstreams.agents.producer.{Producer, NewTransactionProducerPolicy}
import com.bwsw.tstreams.env.TSF_Dictionary
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._


class AManyProducersStreamingInManyRandomPartitionsAndConsumerTest extends FlatSpec
  with Matchers with BeforeAndAfterAll with TestUtils {

  val timeoutForWaiting = 60 * 5
  val totalPartitions = 4
  val totalTxn = 10
  val totalElementsInTxn = 3
  val producersAmount = 10
  val dataToSend = (for (part <- 0 until totalElementsInTxn) yield randomString).sorted

  f.setProperty(TSF_Dictionary.Stream.NAME, "test_stream").
    setProperty(TSF_Dictionary.Stream.PARTITIONS, totalPartitions).
    setProperty(TSF_Dictionary.Stream.TTL, 60 * 10).
    setProperty(TSF_Dictionary.Coordination.CONNECTION_TIMEOUT, 7).
    setProperty(TSF_Dictionary.Coordination.TTL, 7).
    setProperty(TSF_Dictionary.Producer.TRANSPORT_TIMEOUT, 5).
    setProperty(TSF_Dictionary.Producer.Transaction.TTL, 6).
    setProperty(TSF_Dictionary.Producer.Transaction.KEEP_ALIVE, 2).
    setProperty(TSF_Dictionary.Consumer.TRANSACTION_PRELOAD, 10).
    setProperty(TSF_Dictionary.Consumer.DATA_PRELOAD, 10)


  "Some amount of producers and one consumer" should "producers - send transactions in many partition" +
    " (each producer send each txn in only one random partition) " +
    " consumer - retrieve them all" in {

    val producers: List[Producer[String]] =
      (0 until producersAmount)
        .toList
        .map(_ => getProducer(List(scala.util.Random.nextInt(totalPartitions)), totalPartitions))

    val producersThreads = producers.map(p =>
      new Thread(new Runnable {
        def run() {
          var i = 0
          while (i < totalTxn) {
            val txn = p.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
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
      isUseLastOffset = true)

    consumer.start

    val consumerThread = new Thread(
      new Runnable {

        def run() = {
          var i = 0
          while (i < totalTxn * producersAmount) {
            val txn = consumer.getTransaction
            if (txn.isDefined) {
              txn.get.getAll().sorted shouldBe dataToSend
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

    producers.foreach(_.stop())

  }

  def getProducer(usedPartitions: List[Int], totalPartitions: Int): Producer[String] = {
    val port = TestUtils.getPort
    f.setProperty(TSF_Dictionary.Producer.BIND_PORT, port)
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
