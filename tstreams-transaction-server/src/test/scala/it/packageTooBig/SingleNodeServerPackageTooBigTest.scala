package it.packageTooBig

import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException
import com.bwsw.tstreamstransactionserver.netty.client.ClientBuilder
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.SingleNodeServerBuilder
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.TransportOptions
import com.bwsw.tstreamstransactionserver.rpc.{ProducerTransaction, TransactionStates}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import util.Utils
import util.Utils.startZkServerAndGetIt

import scala.concurrent.Await
import scala.concurrent.duration.Duration



class SingleNodeServerPackageTooBigTest
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll {


  private val packageTransmissionOptions = TransportOptions(maxMetadataPackageSize = 1)

  private lazy val serverBuilder = new SingleNodeServerBuilder()
    .withPackageTransmissionOptions(packageTransmissionOptions)

  private lazy val clientBuilder = new ClientBuilder()

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


  "Server" should "not allow client to send a message which has a size that is greater than maxMetadataPackageSize or maxDataPackageSize (throw PackageTooBigException)" in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { _ =>
      val client = bundle.client
      assertThrows[PackageTooBigException] {
        Await.result(client.putProducerState(
          ProducerTransaction(
            1,
            1,
            1L,
            TransactionStates.Opened,
            1,
            10000L
          )

        ), Duration(5, TimeUnit.SECONDS))
      }
    }
  }
}