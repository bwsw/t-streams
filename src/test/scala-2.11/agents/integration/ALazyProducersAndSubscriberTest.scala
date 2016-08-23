package agents.integration

import java.util.UUID
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.consumer.subscriber.{Callback, SubscribingConsumer}
import com.bwsw.tstreams.agents.producer.{NewTransactionProducerPolicy, Producer}
import com.bwsw.tstreams.env.TSF_Dictionary
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._

import scala.collection.mutable.ListBuffer

class ALazyProducersAndSubscriberTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  val timeoutForWaiting = 60
  val totalPartitions = 4
  val totalTxn = 10
  val totalElementsInTxn = 3
  val producersAmount = 10
  val dataToSend = (for (part <- 0 until totalElementsInTxn) yield randomString).sorted
  val lock = new ReentrantLock()
  val map = scala.collection.mutable.Map[Int, ListBuffer[UUID]]()
  val l1 = new CountDownLatch(1)
  val l2 = new CountDownLatch(1)

  f.setProperty(TSF_Dictionary.Stream.NAME,"test_stream").
    setProperty(TSF_Dictionary.Stream.PARTITIONS, 4).
    setProperty(TSF_Dictionary.Stream.TTL, 60 * 10).
    setProperty(TSF_Dictionary.Coordination.CONNECTION_TIMEOUT, 7).
    setProperty(TSF_Dictionary.Coordination.TTL, 7).
    setProperty(TSF_Dictionary.Producer.TRANSPORT_TIMEOUT, 5).
    setProperty(TSF_Dictionary.Producer.Transaction.TTL, 3).
    setProperty(TSF_Dictionary.Producer.Transaction.KEEP_ALIVE, 1).
    setProperty(TSF_Dictionary.Consumer.TRANSACTION_PRELOAD, 10).
    setProperty(TSF_Dictionary.Consumer.DATA_PRELOAD, 10)

  (0 until totalPartitions) foreach { partition =>
    map(partition) = ListBuffer.empty[UUID]
  }

  var cnt = 0

  val producers: List[Producer[String]] =
    (0 until producersAmount)
      .toList
      .map(x => getProducer(List(x % totalPartitions), totalPartitions))

  val producersThreads = producers.map(p =>
    new Thread(new Runnable {
      def run() {
        Thread.currentThread().setName(p.p2pAgent.getAgentAddress)
        p.setAgentName("producer-" + Thread.currentThread().getName)
        var i = 0
        while (i < totalTxn) {
          Thread.sleep(1000)
          val txn = p.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
          dataToSend.foreach(x => txn.send(x))
          txn.checkpoint()
          i += 1
          if(i == 2)
            l1.countDown()
        }
      }
    }))

  val callback = new Callback[String] {
    override def onEvent(subscriber: SubscribingConsumer[String], partition: Int, transactionUuid: UUID): Unit = {
      lock.lock()
      cnt += 1
      map(partition) += transactionUuid
      if(cnt == totalTxn * producersAmount)
        l2.countDown()
      lock.unlock()
    }
  }

  val subscriber = f.getSubscriber[String](
    name = "test_subscriber",
    txnGenerator = LocalGeneratorCreator.getGen(),
    converter = arrayByteToStringConverter,
    partitions = (0 until totalPartitions).toList,
    callback = callback,
    offset = Oldest,
    isUseLastOffset = true)


  "Some amount of producers and subscriber" should "producers - send transactions in many partition" +
    " (each producer send each txn in only one partition without intersection " +
    " for ex. producer1 in partition1, producer2 in partition2, producer3 in partition3 etc...)," +
    " subscriber - retrieve them all(with callback) in sorted order" in {


    producersThreads.foreach(x => x.start())
    l1.await()
    subscriber.start()
    producersThreads.foreach(x => x.join(timeoutForWaiting * 1000L))
    producers.foreach(_.stop())
    val r = l2.await(100000, TimeUnit.MILLISECONDS)
    r shouldBe true
    subscriber.stop()
    assert(map.values.map(x => x.size).sum == totalTxn * producersAmount)
    map foreach { case (_, list) =>
      list.map(x => (x, x.timestamp())).sortBy(_._2).map(x => x._1) shouldEqual list
    }

  }

  def getProducer(usedPartitions: List[Int], totalPartitions: Int): Producer[String] = {
    val port = TestUtils.getPort
    f.setProperty(TSF_Dictionary.Producer.BIND_PORT, port)
    f.getProducer[String](
      name = "test_producer",
      txnGenerator = LocalGeneratorCreator.getGen(),
      converter = stringToArrayByteConverter,
      partitions = (0 until totalPartitions).toList,
      isLowPriority = false)
  }

  override def afterAll(): Unit = {
    onAfterAll()
  }
}