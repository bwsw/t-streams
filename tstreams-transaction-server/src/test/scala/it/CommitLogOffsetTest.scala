package it


import com.bwsw.tstreamstransactionserver.netty.client.ClientBuilder
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.SingleNodeServerBuilder
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions
import com.bwsw.tstreamstransactionserver.rpc.{ConsumerTransaction, ProducerTransaction, TransactionStates}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import util.Utils
import util.Utils.startZkServerAndGetIt

import scala.concurrent.Await
import scala.concurrent.duration._

class CommitLogOffsetTest
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll
{

  private lazy val serverBuilder = new SingleNodeServerBuilder()
    .withCommitLogOptions(SingleNodeServerOptions.CommitLogOptions(
      closeDelayMs = Int.MaxValue
    ))

  private lazy val clientBuilder = new ClientBuilder()


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
      override val quantity: Int = -1
      override val partition: Int = streamObj.partitions
    }

  private def getRandomConsumerTransaction(streamID:Int, streamObj: com.bwsw.tstreamstransactionserver.rpc.StreamValue) =
    new ConsumerTransaction {
      override val transactionID: Long = scala.util.Random.nextLong()
      override val name: String = rand.nextString(1000).toString
      override val stream: Int = streamID
      override val partition: Int = streamObj.partitions
    }

  private lazy val (zkServer, zkClient) =
    startZkServerAndGetIt

  override def beforeAll(): Unit = {
    zkServer
    zkClient
  }

  override def afterAll(): Unit = {
    zkClient.close()
    zkServer.close()
  }



  private val secondsWait = 10

  "getCommitLogOffsets" should "return -1 for currentProcessedCommitLog and 0 for currentConstructedCommitLog as there is created commit log file at initialization of server and it's not processed." in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { _ =>
      val client = bundle.client
      val result = Await.result(client.getCommitLogOffsets(), secondsWait.seconds)
      result.currentProcessedCommitLog shouldBe -1L
      result.currentConstructedCommitLog shouldBe 0L
    }
  }

  it should "return 0 for currentProcessedCommitLog and 1 or more for currentConstructedCommitLog as there is created commit log file at initialization of server, it's closed and server creates a new commit log file and server processes the first one." in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { transactionServer =>
      val client = bundle.client

      val stream = getRandomStream
      val streamID = Await.result(client.putStream(stream), secondsWait.seconds)

      val producerTransactions = Array.fill(10)(getRandomProducerTransaction(streamID, stream)).filter(_.state == TransactionStates.Opened) :+
        getRandomProducerTransaction(streamID, stream).copy(state = TransactionStates.Opened)

      Await.result(client.putTransactions(producerTransactions, Seq()), secondsWait.seconds)

      Await.result(client.putConsumerCheckpoint(getRandomConsumerTransaction(streamID, stream)), secondsWait.seconds)
      transactionServer.scheduledCommitLog.run()
      transactionServer.commitLogToRocksWriter.run()

      val result = Await.result(client.getCommitLogOffsets(), secondsWait.seconds)
      result.currentProcessedCommitLog shouldBe >= (0L)
      result.currentConstructedCommitLog shouldBe >= (1L)
    }
  }

//  it should "return -1 for currentProcessedCommitLog(as writer thread doesn't run on data) and 0 for currentConstructedCommitLog(as it's initialized, got some data, but not closed)" in {
//    val bundle = Utils.startTransactionServerAndClient(
//      zkClient, serverBuilder, clientBuilder
//    )
//
//    bundle.operate { _ =>
//      val client = bundle.client
//
//      val stream = getRandomStream
//      val streamID = Await.result(client.putStream(stream), secondsWait.seconds)
//
//      val producerTransactions = Array.fill(10)(getRandomProducerTransaction(streamID, stream)).filter(_.state == TransactionStates.Opened) :+
//        getRandomProducerTransaction(streamID, stream).copy(state = TransactionStates.Opened)
//
//      Await.result(client.putTransactions(producerTransactions, Seq()), secondsWait.seconds)
//
//      Await.result(client.putConsumerCheckpoint(getRandomConsumerTransaction(streamID, stream)), secondsWait.seconds)
//
//      val result = Await.result(client.getCommitLogOffsets(), secondsWait.seconds)
//      result.currentProcessedCommitLog shouldBe -1L
//      result.currentConstructedCommitLog shouldBe 3L
//    }
//  }

  //  it should "return -1 for currentProcessedCommitLog(as writer thread doesn't run on data) and 1 for currentConstructedCommitLog(as the first one is closed, and a next one is created)" in {
  //    val stream = getRandomStream
  //    Await.result(client.putStream(stream), secondsWait.seconds)
  //
  //    val producerTransactions = Array.fill(10)(getRandomProducerTransaction(stream)).filter(_.state == TransactionStates.Opened) :+
  //      getRandomProducerTransaction(stream).copy(state = TransactionStates.Opened)
  //
  //    Await.result(client.putTransactions(producerTransactions, Seq()), secondsWait.seconds)
  //
  //    TestTimer.updateTime(TestTimer.getCurrentTime + maxIdleTimeBetweenRecordsMs)
  //    Await.result(client.putConsumerCheckpoint(getRandomConsumerTransaction(stream)), secondsWait.seconds)
  //    transactionServer.scheduledCommitLog.run()
  //
  //    val result = Await.result(client.getCommitLogOffsets(), secondsWait.seconds)
  //    result.currentProcessedCommitLog   shouldBe -1L
  //    result.currentConstructedCommitLog shouldBe 1L
  //  }

  //  it should "return -1 for currentProcessedCommitLog(as writer thread doesn't run on data) and 3 for currentConstructedCommitLog(as 3 commit log files are closed)" in {
  //    val stream = getRandomStream
  //    Await.result(client.putStream(stream), secondsWait.seconds)
  //
  //    transactionServer.scheduledCommitLog.run()
  //    transactionServer.scheduledCommitLog.run()
  //    transactionServer.scheduledCommitLog.run()
  //
  //    val result = Await.result(client.getCommitLogOffsets(), secondsWait.seconds)
  //    result.currentProcessedCommitLog   shouldBe -1L
  //    result.currentConstructedCommitLog shouldBe 3L
  //  }
}
