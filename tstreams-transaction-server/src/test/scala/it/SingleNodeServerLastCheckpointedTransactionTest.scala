package it

import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.netty.client.ClientBuilder
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.SingleNodeServerBuilder
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.CommitLogOptions
import com.bwsw.tstreamstransactionserver.rpc.{ConsumerTransaction, ProducerTransaction, TransactionStates}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import util.{Time, Utils}
import util.Utils.startZkServerAndGetIt

import scala.concurrent.duration._
import scala.concurrent.{Await, Future => ScalaFuture}


class SingleNodeServerLastCheckpointedTransactionTest
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll {

  private lazy val (zkServer, zkClient) =
    startZkServerAndGetIt

  private val clientsNum = 2

  private val secondsWait = 10

  private lazy val clientBuilder = new ClientBuilder()
  private lazy val serverBuilder = new SingleNodeServerBuilder()
    .withCommitLogOptions(CommitLogOptions(closeDelayMs = Int.MaxValue))

  private object TestTimer extends Time {
    private val initialTime = System.currentTimeMillis()
    private var currentTime = initialTime

    override def getCurrentTime: Long = currentTime

    def resetTimer(): Unit = currentTime = initialTime

    def updateTime(newTime: Long) = currentTime = newTime
  }

  private val maxIdleTimeBetweenRecordsMs = 10000

  override def beforeAll(): Unit = {
    zkServer
    zkClient
  }

  override def afterAll(): Unit = {
    zkClient.close()
    zkServer.close()
  }

  private val rand = scala.util.Random

  private def getRandomStream =
    new com.bwsw.tstreamstransactionserver.rpc.StreamValue {
      override val name: String = rand.nextInt(10000).toString
      override val partitions: Int = rand.nextInt(10000)
      override val description: Option[String] = if (rand.nextBoolean()) Some(rand.nextInt(10000).toString) else None
      override val ttl: Long = Long.MaxValue
      override val zkPath: Option[String] = None
    }

  private def getRandomConsumerTransaction(streamID: Int, streamObj: com.bwsw.tstreamstransactionserver.rpc.StreamValue) =
    new ConsumerTransaction {
      override val transactionID: Long = scala.util.Random.nextLong()
      override val name: String = rand.nextInt(10000).toString
      override val stream: Int = streamID
      override val partition: Int = streamObj.partitions
    }


  "One client" should "put a stream, then another client should put transactions on the stream on a partition. " +
    "After that the first client tries to put transactions on the stream on the partition and clients should get the same last checkpointed transaction." in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder, clientsNum
    )

    bundle.operate { transactionServer =>

      val stream = getRandomStream

      val firstClient = bundle.clients(0)
      val secondClient = bundle.clients(1)

      val streamID = Await.result(firstClient.putStream(stream), secondsWait.seconds)
      streamID shouldNot be (-1)

      Await.result(secondClient.delStream(stream.name), secondsWait.seconds) shouldBe true

      val streamUpdated = stream.copy(description = Some("I replace a previous one."))
      Await.result(secondClient.putStream(streamUpdated), secondsWait.seconds) shouldBe (streamID + 1)


      TestTimer.updateTime(TestTimer.getCurrentTime + TimeUnit.SECONDS.toMillis(1))

      //transactions are processed in the async mode
      val producerTransaction1 = ProducerTransaction(streamID, stream.partitions, TestTimer.getCurrentTime, TransactionStates.Opened, -1, Long.MaxValue)

      Await.result(secondClient.putProducerState(producerTransaction1), secondsWait.seconds) shouldBe true
      Await.result(secondClient.putProducerState(producerTransaction1.copy(state = TransactionStates.Checkpointed)), secondsWait.seconds) shouldBe true
      Await.result(firstClient.getLastCheckpointedTransaction(streamID, stream.partitions), secondsWait.seconds) shouldBe -1L
      Await.result(secondClient.getLastCheckpointedTransaction(streamID, stream.partitions), secondsWait.seconds) shouldBe -1L

      //it's required to close a current commit log file
      TestTimer.updateTime(TestTimer.getCurrentTime + maxIdleTimeBetweenRecordsMs)
      Await.result(firstClient.putConsumerCheckpoint(getRandomConsumerTransaction(streamID, stream)), secondsWait.seconds)
      //it's required to a CommitLogToBerkeleyWriter writes the producer transactions to db
      transactionServer.scheduledCommitLog.run()
      transactionServer.commitLogToRocksWriter.run()

      Await.result(firstClient.getLastCheckpointedTransaction(streamID, stream.partitions), secondsWait.seconds) shouldBe producerTransaction1.transactionID
      Await.result(secondClient.getLastCheckpointedTransaction(streamID, stream.partitions), secondsWait.seconds) shouldBe producerTransaction1.transactionID

      val producerTransaction2 = ProducerTransaction(streamID, stream.partitions, TestTimer.getCurrentTime, TransactionStates.Opened, -1, Long.MaxValue)
      Await.result(secondClient.putProducerState(producerTransaction2.copy()), secondsWait.seconds) shouldBe true
      Await.result(secondClient.putProducerState(producerTransaction2.copy(state = TransactionStates.Checkpointed)), secondsWait.seconds) shouldBe true
      Await.result(secondClient.putProducerState(producerTransaction2.copy(state = TransactionStates.Opened, transactionID = TestTimer.getCurrentTime + 1L)), secondsWait.seconds) shouldBe true

      Await.result(firstClient.getLastCheckpointedTransaction(streamID, stream.partitions), secondsWait.seconds) shouldBe producerTransaction1.transactionID
      Await.result(secondClient.getLastCheckpointedTransaction(streamID, stream.partitions), secondsWait.seconds) shouldBe producerTransaction1.transactionID


      //it's required to close a current commit log file
      TestTimer.updateTime(TestTimer.getCurrentTime + maxIdleTimeBetweenRecordsMs)
      Await.result(firstClient.putConsumerCheckpoint(getRandomConsumerTransaction(streamID, stream)), secondsWait.seconds)
      //it's required to a CommitLogToBerkeleyWriter writes the producer transactions to db
      transactionServer.scheduledCommitLog.run()
      transactionServer.commitLogToRocksWriter.run()

      Await.result(firstClient.getLastCheckpointedTransaction(streamID, stream.partitions), secondsWait.seconds) shouldBe producerTransaction2.transactionID
      Await.result(secondClient.getLastCheckpointedTransaction(streamID, stream.partitions), secondsWait.seconds) shouldBe producerTransaction2.transactionID
    }
  }

  it should "return last checkpointed transaction" in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { transactionServer =>

      val ALL = 100
      val transactions = for (i <- 0 until ALL) yield {
        TestTimer.updateTime(TestTimer.getCurrentTime + 1L)
        TestTimer.getCurrentTime
      }
      val transaction = transactions.head

      val client = bundle.client

      val stream = com.bwsw.tstreamstransactionserver.rpc.StreamValue("test_stream", 32, None, 360)
      val streamID = Await.result(client.putStream(stream), secondsWait.seconds)

      val partition = 1
      Await.result(client.putProducerState(ProducerTransaction(streamID, partition, transactions.head, TransactionStates.Opened, -1, 120)), secondsWait.seconds) shouldBe true
      Await.result(client.putProducerState(ProducerTransaction(streamID, partition, transactions.head, TransactionStates.Checkpointed, -1, 120)), secondsWait.seconds) shouldBe true

      Await.ready(
        ScalaFuture.sequence(
          transactions.drop(1).map(t => client.putProducerState(ProducerTransaction(streamID, partition, t, TransactionStates.Opened, -1, 120)))
        )(implicitly, scala.concurrent.ExecutionContext.Implicits.global), secondsWait.seconds)


      //it's required to close a current commit log file
      TestTimer.updateTime(TestTimer.getCurrentTime + maxIdleTimeBetweenRecordsMs)
      Await.result(client.putConsumerCheckpoint(getRandomConsumerTransaction(streamID, stream)), secondsWait.seconds)
      //it's required to a CommitLogToBerkeleyWriter writes the producer transactions to db
      transactionServer.scheduledCommitLog.run()
      transactionServer.commitLogToRocksWriter.run()


      val retrievedTransaction = Await.result(client.getLastCheckpointedTransaction(streamID, partition), secondsWait.seconds)
      retrievedTransaction shouldEqual transaction
    }
  }
}
