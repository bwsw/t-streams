package it


import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreamstransactionserver.netty.client.ClientBuilder
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.SingleNodeServerBuilder
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions
import com.bwsw.tstreamstransactionserver.rpc.ConsumerTransaction
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import util.Utils
import util.Utils.startZkServerAndGetIt

import scala.concurrent.Await
import scala.concurrent.duration._

class SingleNodeServerConsumerTransactionNotificationTest
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll {

  private val commitLogToBerkeleyDBTaskDelayMs = 100
  private lazy val serverBuilder = new SingleNodeServerBuilder()
    .withCommitLogOptions(SingleNodeServerOptions.CommitLogOptions(
      closeDelayMs = commitLogToBerkeleyDBTaskDelayMs
    ))

  private lazy val (zkServer, zkClient) =
    startZkServerAndGetIt

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


  override def beforeAll(): Unit = {
    zkServer
    zkClient
  }

  override def afterAll(): Unit = {
    zkClient.close()
    zkServer.close()
  }

  private val secondsWait = 10

  "Client" should "put consumerCheckpoint and get a transaction id back." in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { transactionServer =>
      val client = bundle.client

      val stream = getRandomStream
      val streamID = Await.result(client.putStream(stream), secondsWait.seconds)

      val transactionId = 10L
      val checkpointName = "test-name"


      val latch = new CountDownLatch(1)
      transactionServer.notifyConsumerTransactionCompleted(consumerTransaction =>
        consumerTransaction.transactionID == transactionId && consumerTransaction.name == checkpointName, latch.countDown()
      )

      val consumerTransactionOuter = ConsumerTransaction(streamID, 1, transactionId, checkpointName)
      client.putConsumerCheckpoint(consumerTransactionOuter)

      latch.await(1, TimeUnit.SECONDS) shouldBe true
    }
  }

  it should "shouldn't get notification." in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { transactionServer =>
      val client = bundle.client
      val stream = getRandomStream
      Await.result(client.putStream(stream), secondsWait.seconds)

      val transactionId = 10L
      val checkpointName = "test-name"

      val latch = new CountDownLatch(1)
      val id = transactionServer.notifyConsumerTransactionCompleted(consumerTransaction =>
        consumerTransaction.transactionID == transactionId && consumerTransaction.name == checkpointName, latch.countDown()
      )

      latch.await(1, TimeUnit.SECONDS) shouldBe false
      transactionServer.removeConsumerNotification(id) shouldBe true
    }
  }

  it should "get notification about consumer checkpoint after using putTransactions method." in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { transactionServer =>
      val client = bundle.client
      val stream = getRandomStream
      val streamID = Await.result(client.putStream(stream), secondsWait.seconds)

      val transactionId = 10L
      val checkpointName = "test-name"


      val latch = new CountDownLatch(1)
      transactionServer.notifyConsumerTransactionCompleted(consumerTransaction =>
        consumerTransaction.transactionID == transactionId, latch.countDown()
      )

      val consumerTransactionOuter = ConsumerTransaction(streamID, 1, transactionId, checkpointName)
      client.putTransactions(Seq(), Seq(consumerTransactionOuter))

      latch.await(15, TimeUnit.SECONDS) shouldBe true
    }
  }

}
