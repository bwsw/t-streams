package it

import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.netty.client.ClientBuilder
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.SingleNodeServerBuilder
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions
import com.bwsw.tstreamstransactionserver.rpc.{ConsumerTransaction, ProducerTransaction, TransactionStates}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import util.{Time, Utils}
import util.Utils.startZkServerAndGetIt

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

class ManyClientsSingleNodeServerInterconnectionTest
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll {

  private val clientsNum = 2

  private object TestTimer extends Time {
    private val initialTime = System.currentTimeMillis()
    private var currentTime = initialTime

    override def getCurrentTime: Long = currentTime

    def resetTimer(): Unit = currentTime = initialTime

    def updateTime(newTime: Long) = currentTime = newTime
  }

  private val maxIdleTimeBetweenRecordsMs = 10000

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
      override val ttl: Long = 5
      override val zkPath: Option[String] = None
    }

  private def getRandomProducerTransaction(streamID: Int,
                                           streamObj: com.bwsw.tstreamstransactionserver.rpc.StreamValue,
                                           transactionState: TransactionStates = TransactionStates(rand.nextInt(TransactionStates.list.length) + 1),
                                           id: Long = System.nanoTime()) =
    ProducerTransaction(
      streamID,
      streamObj.partitions,
      id,
      transactionState,
      -1,
      25000L
    )

  private def getRandomConsumerTransaction(streamID: Int, streamObj: com.bwsw.tstreamstransactionserver.rpc.StreamValue) =
    ConsumerTransaction(
      streamID,
      streamObj.partitions,
      scala.util.Random.nextLong(),
      rand.nextInt(10000).toString
    )


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

  "One client" should "put stream, then another client should delete it. After that the first client tries to put transactions and doesn't get an exception." in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder, clientsNum
    )

    bundle.operate { transactionServer =>
      val clients = bundle.clients

      val stream = getRandomStream

      val firstClient = clients(0)
      val secondClient = clients(1)

      val streamID = Await.result(firstClient.putStream(stream), secondsWait.seconds)
      streamID shouldNot be (-1)
      val producerTransactions = Array.fill(100)(getRandomProducerTransaction(streamID, stream))
        .filter(_.state == TransactionStates.Opened)
      val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(streamID, stream))

      val streamUpdated = stream.copy(description = Some("I replace a previous one."))

      Await.result(secondClient.delStream(stream.name), secondsWait.seconds) shouldBe true
      Await.result(secondClient.putStream(streamUpdated), secondsWait.seconds) shouldBe (streamID + 1)


      //transactions are processed in the async mode
      Await.result(firstClient.putTransactions(producerTransactions, consumerTransactions), secondsWait.seconds) shouldBe true


      //it's required to close a current commit log file
      transactionServer.scheduledCommitLog.run()
      //it's required to a CommitLogToBerkeleyWriter writes the producer transactions to db
      transactionServer.commitLogToRocksWriter.run()

      val fromID = producerTransactions.minBy(_.transactionID).transactionID
      val toID = producerTransactions.maxBy(_.transactionID).transactionID

      Await.result(firstClient.scanTransactions(streamID, stream.partitions, fromID, toID, Int.MaxValue, Set()), secondsWait.seconds)
        .producerTransactions should not be empty
    }
  }

  "One client" should "put stream, then another client should delete it and put with the same name. " +
    "After that the first client should put transactions and get them back in the appropriate quantity." in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder, clientsNum
    )

    bundle.operate { transactionServer =>
      val clients = bundle.clients

      val stream = getRandomStream
      val txnNumber = 100
      val firstClient = clients(0)
      val secondClient = clients(1)

      val streamID = Await.result(firstClient.putStream(stream), secondsWait.seconds)
      streamID shouldNot be (-1)
      val producerTransactions = Array.fill(txnNumber)(getRandomProducerTransaction(streamID, stream, TransactionStates.Opened))
      val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(streamID, stream))

      val streamUpdated = stream.copy(description = Some("I replace a previous one."))

      Await.result(secondClient.delStream(stream.name), secondsWait.seconds) shouldBe true
      Await.result(secondClient.putStream(streamUpdated), secondsWait.seconds) shouldBe (streamID + 1)

      val currentStream = Await.result(firstClient.getStream(stream.name), secondsWait.seconds)
      currentStream shouldBe defined

      //transactions are processed in the async mode
      Await.result(firstClient.putTransactions(
        producerTransactions flatMap (producerTransaction =>
          Seq(producerTransaction, producerTransaction.copy(state = TransactionStates.Checkpointed))),
        consumerTransactions
      ), secondsWait.seconds) shouldBe true

      //it's required to close a current commit log file
      TestTimer.updateTime(System.currentTimeMillis() + maxIdleTimeBetweenRecordsMs)
      Await.result(firstClient.putConsumerCheckpoint(getRandomConsumerTransaction(streamID, stream)), secondsWait.seconds)
      //it's required to a CommitLogToBerkeleyWriter writes the producer transactions to db
      transactionServer.scheduledCommitLog.run()
      transactionServer.commitLogToRocksWriter.run()

      val fromID = producerTransactions.head.transactionID
      val toID = producerTransactions.last.transactionID

      Await.result(firstClient.scanTransactions(streamID, stream.partitions, fromID, toID,
        Int.MaxValue, Set(TransactionStates.Opened)), secondsWait.seconds).producerTransactions should have size txnNumber
    }
  }


  "One client" should "put producer transaction: Opened; the second one: Canceled. The Result is invalid transaction." in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder, clientsNum
    )

    bundle.operate { transactionServer =>
      val clients = bundle.clients
      val stream = getRandomStream

      val firstClient = clients(0)
      val secondClient = clients(1)

      val streamID = Await.result(firstClient.putStream(stream), secondsWait.seconds)

      TestTimer.updateTime(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(1))

      //transactions are processed in the async mode
      val producerTransaction1 = ProducerTransaction(streamID, stream.partitions, TestTimer.getCurrentTime, TransactionStates.Opened, -1, Long.MaxValue)
      Await.result(secondClient.putProducerState(producerTransaction1), secondsWait.seconds) shouldBe true

      //it's required to close a current commit log file
      TestTimer.updateTime(TestTimer.getCurrentTime + maxIdleTimeBetweenRecordsMs)
      Await.result(firstClient.putConsumerCheckpoint(getRandomConsumerTransaction(streamID, stream)), secondsWait.seconds)
      //it's required to a CommitLogToBerkeleyWriter writes the producer transactions to db
      transactionServer.scheduledCommitLog.run()
      transactionServer.commitLogToRocksWriter.run()

      val canceledTransaction = producerTransaction1.copy(state = TransactionStates.Cancel, quantity = 0, ttl = 0L)
      Await.result(secondClient.putProducerState(canceledTransaction), secondsWait.seconds) shouldBe true

      //it's required to close a current commit log file
      TestTimer.updateTime(TestTimer.getCurrentTime + maxIdleTimeBetweenRecordsMs)
      Await.result(firstClient.putConsumerCheckpoint(getRandomConsumerTransaction(streamID, stream)), secondsWait.seconds)
      //it's required to a CommitLogToBerkeleyWriter writes the producer transactions to db
      transactionServer.scheduledCommitLog.run()
      transactionServer.commitLogToRocksWriter.run()

      Await.result(firstClient.getTransaction(streamID, stream.partitions, producerTransaction1.transactionID), secondsWait.seconds).transaction.get shouldBe canceledTransaction.copy(state = TransactionStates.Invalid)
      Await.result(secondClient.getTransaction(streamID, stream.partitions, producerTransaction1.transactionID), secondsWait.seconds).transaction.get shouldBe canceledTransaction.copy(state = TransactionStates.Invalid)
    }
  }

  "One client" should "put producer transaction: Opened; the second one: Checkpointed. The Result is checkpointed transaction." in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder, clientsNum
    )

    bundle.operate { transactionServer =>
      val clients = bundle.clients

      val stream = getRandomStream

      val firstClient = clients(0)
      val secondClient = clients(1)

      val streamID = Await.result(firstClient.putStream(stream), secondsWait.seconds)

      TestTimer.updateTime(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(1))

      //transactions are processed in the async mode
      val producerTransaction1 = ProducerTransaction(streamID, stream.partitions, TestTimer.getCurrentTime, TransactionStates.Opened, -1, maxIdleTimeBetweenRecordsMs * 10)
      Await.result(secondClient.putProducerState(producerTransaction1), secondsWait.seconds) shouldBe true

      TestTimer.updateTime(TestTimer.getCurrentTime + maxIdleTimeBetweenRecordsMs)
      //it's required to close a current commit log file
      transactionServer.scheduledCommitLog.run()
      //it's required to a CommitLogToBerkeleyWriter writes the producer transactions to db
      transactionServer.commitLogToRocksWriter.run()

      TestTimer.updateTime(TestTimer.getCurrentTime + maxIdleTimeBetweenRecordsMs)
      val checkpointedTransaction = producerTransaction1.copy(state = TransactionStates.Checkpointed)
      Await.result(secondClient.putProducerState(checkpointedTransaction), secondsWait.seconds) shouldBe true

      //it's required to close a current commit log file
      transactionServer.scheduledCommitLog.run()
      //it's required to a CommitLogToBerkeleyWriter writes the producer transactions to db
      transactionServer.commitLogToRocksWriter.run()

      Await.result(firstClient.getTransaction(streamID, stream.partitions, producerTransaction1.transactionID), secondsWait.seconds).transaction.get shouldBe checkpointedTransaction
      Await.result(secondClient.getTransaction(streamID, stream.partitions, producerTransaction1.transactionID), secondsWait.seconds).transaction.get shouldBe checkpointedTransaction
    }
  }

  "One client" should "put producer transaction: Opened; the second one: Invalid. The Result is opened transaction." in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder, clientsNum
    )

    bundle.operate { transactionServer =>
      val clients = bundle.clients

      val stream = getRandomStream

      val firstClient = clients(0)
      val secondClient = clients(1)

      val streamID = Await.result(firstClient.putStream(stream), secondsWait.seconds)

      TestTimer.updateTime(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(1))

      //transactions are processed in the async mode
      val producerTransaction1 = ProducerTransaction(streamID, stream.partitions, TestTimer.getCurrentTime, TransactionStates.Opened, -1, Long.MaxValue)
      Await.result(secondClient.putProducerState(producerTransaction1), secondsWait.seconds) shouldBe true

      //it's required to close a current commit log file
      TestTimer.updateTime(TestTimer.getCurrentTime + maxIdleTimeBetweenRecordsMs)
      Await.result(firstClient.putConsumerCheckpoint(getRandomConsumerTransaction(streamID, stream)), secondsWait.seconds)
      //it's required to a CommitLogToBerkeleyWriter writes the producer transactions to db
      transactionServer.scheduledCommitLog.run()
      transactionServer.commitLogToRocksWriter.run()

      val invalidTransaction = producerTransaction1.copy(state = TransactionStates.Invalid)
      Await.result(secondClient.putProducerState(invalidTransaction), secondsWait.seconds) shouldBe true

      //it's required to close a current commit log file
      TestTimer.updateTime(TestTimer.getCurrentTime + maxIdleTimeBetweenRecordsMs)
      Await.result(firstClient.putConsumerCheckpoint(getRandomConsumerTransaction(streamID, stream)), secondsWait.seconds)
      //it's required to a CommitLogToBerkeleyWriter writes the producer transactions to db
      transactionServer.scheduledCommitLog.run()
      transactionServer.commitLogToRocksWriter.run()

      Await.result(firstClient.getTransaction(streamID, stream.partitions, producerTransaction1.transactionID), secondsWait.seconds).transaction.get shouldBe producerTransaction1
      Await.result(secondClient.getTransaction(streamID, stream.partitions, producerTransaction1.transactionID), secondsWait.seconds).transaction.get shouldBe producerTransaction1
    }
  }

  "One client" should "put transaction data, another one should put the data with intersecting keys, and, as consequence, overwrite values." in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder, clientsNum
    )

    bundle.operate { transactionServer =>
      val clients = bundle.clients
      val stream = getRandomStream

      val dataAmount = 10

      val firstClient = clients(0)
      val secondClient = clients(1)

      val streamID = Await.result(firstClient.putStream(stream), secondsWait.seconds)
      val producerTransaction = getRandomProducerTransaction(streamID, stream)

      val data1 = Array.fill(dataAmount)(("a" + new String(rand.nextInt(100000).toString)).getBytes)
      Await.result(firstClient.putTransactionData(streamID, stream.partitions, producerTransaction.transactionID, data1, 0), secondsWait.seconds) shouldBe true
      val data1Retrieved = Await.result(firstClient.getTransactionData(streamID, stream.partitions, producerTransaction.transactionID, 0, dataAmount - 1), secondsWait.seconds)

      val data2 = Array.fill(dataAmount)(("b" + new String(rand.nextInt(100000).toString)).getBytes)
      Await.result(secondClient.putTransactionData(streamID, stream.partitions, producerTransaction.transactionID, data2, 0), secondsWait.seconds) shouldBe true
      val data2Retrieved = Await.result(secondClient.getTransactionData(streamID, stream.partitions, producerTransaction.transactionID, 0, dataAmount - 1), secondsWait.seconds)

      data1Retrieved should not contain theSameElementsInOrderAs(data2Retrieved)
    }
  }

  "One client" should "put transaction data, another one should put the data for which a \"from\" value equals the previous \"to\" value, and both clients should get the concatenation of this data." in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder, clientsNum
    )

    bundle.operate { transactionServer =>
      val clients = bundle.clients

      val stream = getRandomStream

      val dataAmount = 10

      val firstClient = clients(0)
      val secondClient = clients(1)

      val streamID = Await.result(firstClient.putStream(stream), secondsWait.seconds)
      val producerTransaction = getRandomProducerTransaction(streamID, stream)

      val data1 = Array.fill(dataAmount)(("a" + new String(rand.nextInt(100000).toString)).getBytes)
      Await.result(firstClient.putTransactionData(streamID, stream.partitions, producerTransaction.transactionID, data1, 0), secondsWait.seconds) shouldBe true

      val data2 = Array.fill(dataAmount)(("b" + new String(rand.nextInt(100000).toString)).getBytes)
      Await.result(secondClient.putTransactionData(streamID, stream.partitions, producerTransaction.transactionID, data2, dataAmount), secondsWait.seconds) shouldBe true

      val data1Retrieved = Await.result(firstClient.getTransactionData(streamID, stream.partitions, producerTransaction.transactionID, 0, dataAmount - 1), secondsWait.seconds)
      val data2Retrieved = Await.result(secondClient.getTransactionData(streamID, stream.partitions, producerTransaction.transactionID, dataAmount, 2 * dataAmount - 1), secondsWait.seconds)

      data1Retrieved should contain theSameElementsInOrderAs data1
      data2Retrieved should contain theSameElementsInOrderAs data2
      data1Retrieved should not contain theSameElementsInOrderAs(data2Retrieved)
    }
  }
}
