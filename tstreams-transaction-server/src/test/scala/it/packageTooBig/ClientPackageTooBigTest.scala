package it.packageTooBig

import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException
import com.bwsw.tstreamstransactionserver.netty.client.ClientBuilder
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.SingleNodeServerBuilder
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.TransportOptions
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import util.Utils
import util.Utils.startZkServerAndGetIt

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class ClientPackageTooBigTest
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll {

  private val packageTransmissionOptions =
    TransportOptions(maxMetadataPackageSize = 1000000)

  private lazy val serverBuilder = new SingleNodeServerBuilder()
    .withPackageTransmissionOptions(packageTransmissionOptions)

  private lazy val clientBuilder = new ClientBuilder()

  private lazy val (zkServer, zkClient) =
    startZkServerAndGetIt

  private val secondsToWait = 10

  override def beforeAll(): Unit = {
    zkServer
    zkClient
  }

  override def afterAll(): Unit = {
    zkClient.close()
    zkServer.close()
  }

  "Client" should "not allow to transmit amount of data that is greater than maxMetadataPackageSize or maxDataPackageSize (throw PackageTooBigException)" in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { _ =>
      val client = bundle.client
      assertThrows[PackageTooBigException] {
        Await.result(client.putTransactionData(
          1,
          1,
          1L,
          Array.fill(2)(new Array[Byte](packageTransmissionOptions.maxMetadataPackageSize)),
          1
        ), Duration(secondsToWait, TimeUnit.SECONDS))
      }

    }
  }
}
