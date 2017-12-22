package it.multinode

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreamstransactionserver.netty.client.ClientBuilder
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.CommonCheckpointGroupServerBuilder
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions.BookkeeperOptions
import com.bwsw.tstreamstransactionserver.rpc.{ConsumerTransaction, ProducerTransaction, TransactionInfo, TransactionStates}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import util.Implicit.ProducerTransactionSortable

import scala.concurrent.duration._
import scala.concurrent.Await

class CommonCheckpointGroupServerTest
  extends FlatSpec
    with BeforeAndAfterAll
    with Matchers {

  private val rand = scala.util.Random

  private def getRandomStream =
    new com.bwsw.tstreamstransactionserver.rpc.StreamValue {
      override val name: String = rand.nextInt(10000).toString
      override val partitions: Int = rand.nextInt(10000)
      override val description: Option[String] = if (rand.nextBoolean()) Some(rand.nextInt(10000).toString) else None
      override val ttl: Long = Long.MaxValue
      override val zkPath: Option[String] = None
    }

  private def getRandomProducerTransaction(streamID: Int,
                                           streamObj: com.bwsw.tstreamstransactionserver.rpc.StreamValue,
                                           transactionState: TransactionStates = TransactionStates(rand.nextInt(TransactionStates.list.length) + 1),
                                           id: Long = System.nanoTime()) =
    new ProducerTransaction {
      override val transactionID: Long = id
      override val state: TransactionStates = transactionState
      override val stream: Int = streamID
      override val ttl: Long = Long.MaxValue
      override val quantity: Int = 0
      override val partition: Int = streamObj.partitions
    }

  private def getRandomConsumerTransaction(streamID: Int, streamObj: com.bwsw.tstreamstransactionserver.rpc.StreamValue) =
    new ConsumerTransaction {
      override val transactionID: Long = scala.util.Random.nextLong()
      override val name: String = rand.nextInt(10000).toString
      override val stream: Int = streamID
      override val partition: Int = streamObj.partitions
    }

  private val ensembleNumber = 3
  private val writeQourumNumber = 3
  private val ackQuorumNumber = 2

  private val bookkeeperOptions =
    BookkeeperOptions(
      ensembleNumber,
      writeQourumNumber,
      ackQuorumNumber,
      "test".getBytes()
    )

  private val maxIdleTimeBetweenRecordsMs = 1000
  private lazy val serverBuilder = new CommonCheckpointGroupServerBuilder()
  private lazy val clientBuilder = new ClientBuilder()


  private val bookiesNumber =
    ensembleNumber max writeQourumNumber max ackQuorumNumber


  private lazy val (zkServer, zkClient, bookieServers) =
    util.Utils.startZkServerBookieServerZkClient(bookiesNumber)

  override def beforeAll(): Unit = {
    zkServer
    zkClient
    bookieServers
  }

  override def afterAll(): Unit = {
    bookieServers.foreach(_.shutdown())
    zkClient.close()
    zkServer.close()
  }


  val secondsWait = 15

  it should "[scanTransactions] put transactions and get them back" in {
    val bundle = util.multiNode.Util.getCommonCheckpointGroupServerBundle(
      zkClient, bookkeeperOptions, serverBuilder, clientBuilder, maxIdleTimeBetweenRecordsMs
    )

    bundle.operate { transactionServer =>
      val client = bundle.client
      val stream = getRandomStream
      val streamID = Await.result(client.putStream(stream), secondsWait.seconds)


      val producerTransactions = Array.fill(30)(getRandomProducerTransaction(streamID, stream)).filter(_.state == TransactionStates.Opened) :+
        getRandomProducerTransaction(streamID, stream).copy(state = TransactionStates.Opened)

      val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(streamID, stream))

      val statesAllowed = Array(TransactionStates.Opened, TransactionStates.Updated)
      val (from, to) = (
        producerTransactions.filter(txn => statesAllowed.contains(txn.state)).minBy(_.transactionID).transactionID,
        producerTransactions.filter(txn => statesAllowed.contains(txn.state)).maxBy(_.transactionID).transactionID
      )

      val latch = new CountDownLatch(1)
      transactionServer.notifyProducerTransactionCompleted(
        txn => txn.transactionID == to,
        latch.countDown()
      )

      Await.result(client.putTransactions(producerTransactions, consumerTransactions), secondsWait.seconds)


      latch.await(secondsWait, TimeUnit.SECONDS) shouldBe true

      val resFrom_1From = Await.result(client.scanTransactions(streamID, stream.partitions, from - 1, from, Int.MaxValue, Set()), secondsWait.seconds)
      resFrom_1From.producerTransactions.size shouldBe 1
      resFrom_1From.producerTransactions.head.transactionID shouldBe from


      val resFromFrom = Await.result(client.scanTransactions(streamID, stream.partitions, from, from, Int.MaxValue, Set()), secondsWait.seconds)
      resFromFrom.producerTransactions.size shouldBe 1
      resFromFrom.producerTransactions.head.transactionID shouldBe from


      val resToFrom = Await.result(client.scanTransactions(streamID, stream.partitions, to, from, Int.MaxValue, Set()), secondsWait.seconds)
      resToFrom.producerTransactions.size shouldBe 0

      val producerTransactionsByState = producerTransactions.groupBy(_.state)
      val res = Await.result(client.scanTransactions(streamID, stream.partitions, from, to, Int.MaxValue, Set()), secondsWait.seconds).producerTransactions

      val producerOpenedTransactions = producerTransactionsByState(TransactionStates.Opened).sortBy(_.transactionID)

      res.head shouldBe producerOpenedTransactions.head
      res shouldBe sorted
    }
  }

  it should "put producer and consumer transactions" in {
    val bundle = util.multiNode.Util.getCommonCheckpointGroupServerBundle(
      zkClient, bookkeeperOptions, serverBuilder, clientBuilder, maxIdleTimeBetweenRecordsMs
    )

    bundle.operate { _ =>
      val client = bundle.client

      val stream = getRandomStream
      val streamID = Await.result(client.putStream(stream), secondsWait.seconds)

      val producerTransactions = Array.fill(100)(getRandomProducerTransaction(streamID, stream))
      val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(streamID, stream))

      val result = client.putTransactions(producerTransactions, consumerTransactions)

      Await.result(result, 5.seconds) shouldBe true
    }
  }

  it should "put any kind of binary data and get it back" in {
    val bundle = util.multiNode.Util.getCommonCheckpointGroupServerBundle(
      zkClient, bookkeeperOptions, serverBuilder, clientBuilder, maxIdleTimeBetweenRecordsMs
    )

    bundle.operate { _ =>
      val client = bundle.client


      val stream = getRandomStream
      val streamID = Await.result(client.putStream(stream), secondsWait.seconds)
      streamID shouldNot be (-1)

      val txn = getRandomProducerTransaction(streamID, stream)
      Await.result(client.putProducerState(txn), secondsWait.seconds)

      val dataAmount = 5000
      val data = Array.fill(dataAmount)(rand.nextString(10).getBytes)

      val resultInFuture = Await.result(client.putTransactionData(streamID, txn.partition, txn.transactionID, data, 0), secondsWait.seconds)
      resultInFuture shouldBe true

      val currentOffset = Await.result(client.getCommitLogOffsets(), secondsWait.seconds)
      var isNotOffsetOvercome = true
      while (isNotOffsetOvercome) {
        TimeUnit.MILLISECONDS.sleep(maxIdleTimeBetweenRecordsMs)
        val res =
          Await.result(client.getCommitLogOffsets(), secondsWait.seconds)

        isNotOffsetOvercome =
          currentOffset.currentConstructedCommitLog + 1 > res.currentProcessedCommitLog
      }

      val dataFromDatabase = Await.result(client.getTransactionData(streamID, txn.partition, txn.transactionID, 0, dataAmount), secondsWait.seconds)
      data should contain theSameElementsAs dataFromDatabase
    }
  }

  it should "[putProducerStateWithData] put a producer transaction (Opened) with data, and server should persist data." in {
    val bundle = util.multiNode.Util.getCommonCheckpointGroupServerBundle(
      zkClient, bookkeeperOptions, serverBuilder, clientBuilder, maxIdleTimeBetweenRecordsMs
    )

    bundle.operate { transactionServer =>
      val client = bundle.client

      //arrange
      val stream =
        getRandomStream

      val streamID =
        Await.result(client.putStream(stream), secondsWait.seconds)

      val openedProducerTransaction =
        getRandomProducerTransaction(streamID, stream, TransactionStates.Opened)

      val dataAmount = 30
      val data = Array.fill(dataAmount)(rand.nextString(10).getBytes)

      val latch = new CountDownLatch(1)
      transactionServer.notifyProducerTransactionCompleted(
        txn => txn.transactionID == openedProducerTransaction.transactionID,
        latch.countDown()
      )


      val from = dataAmount
      val to = 2 * from
      Await.result(client.putProducerStateWithData(openedProducerTransaction, data, from), secondsWait.seconds)

      latch.await(secondsWait, TimeUnit.SECONDS) shouldBe true

      val successResponse = Await.result(
        client.getTransaction(streamID, stream.partitions, openedProducerTransaction.transactionID
        ), secondsWait.seconds)

      val successResponseData = Await.result(
        client.getTransactionData(
          streamID, stream.partitions, openedProducerTransaction.transactionID, from, to
        ), secondsWait.seconds)


      //assert
      successResponse shouldBe TransactionInfo(
        exists = true,
        Some(openedProducerTransaction)
      )

      successResponseData should contain theSameElementsInOrderAs data
    }
  }
}
