package it

import com.bwsw.tstreamstransactionserver.netty.client.ClientBuilder
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.SingleNodeServerBuilder
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import util.Utils
import util.Utils.startZkServerAndGetIt

import scala.concurrent.Await
import scala.concurrent.duration._

class SingleNodeServerGetTransactionTest
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll
{
  private val secondsToWait = 10.seconds

  private lazy val serverBuilder =
    new SingleNodeServerBuilder()

  private lazy val clientBuilder =
    new ClientBuilder()


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

  "Client" should "get transaction ID that not less that current time" in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { _ =>
      val client = bundle.client


      val currentTime = System.currentTimeMillis()
      val result = Await.result(client.getTransaction(), secondsToWait)


      result shouldBe >=(currentTime)
    }
  }

  it should "get transaction ID by timestamp" in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { _ =>
      val client = bundle.client

      val currentTime = System.currentTimeMillis()
      val result = Await.result(client.getTransaction(currentTime), secondsToWait)

      result shouldBe (currentTime * 100000)
    }
  }

}
