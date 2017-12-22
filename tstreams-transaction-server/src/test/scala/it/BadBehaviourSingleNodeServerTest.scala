package it

import java.util.concurrent.atomic.AtomicInteger

import com.bwsw.tstreamstransactionserver.netty.SocketHostPortPair
import com.bwsw.tstreamstransactionserver.netty.client.Client
import com.bwsw.tstreamstransactionserver.netty.server.zk.{ZKMasterElector, ZookeeperClient}
import com.bwsw.tstreamstransactionserver.options.ClientOptions.{AuthOptions, ConnectionOptions}
import com.bwsw.tstreamstransactionserver.options.{ClientOptions, CommonOptions}
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.channel.socket.SocketChannel
import org.apache.curator.retry.RetryForever
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import util.Utils
import util.Utils.startZkServerAndGetIt
import util.netty.NettyServer

import scala.concurrent.{Await, Promise}
import scala.concurrent.duration._

class BadBehaviourSingleNodeServerTest
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll {

  private lazy val (zkServer, zkClient) =
    startZkServerAndGetIt

  private val host = "127.0.0.1"

  private def uuid = util.Utils.uuid


  private val rand = scala.util.Random

  private def getRandomStream = new com.bwsw.tstreamstransactionserver.rpc.StreamValue {
    override val name: String = rand.nextInt(10000).toString
    override val partitions: Int = rand.nextInt(10000)
    override val description: Option[String] = if (rand.nextBoolean()) Some(rand.nextInt(10000).toString) else None
    override val ttl: Long = Long.MaxValue

    override def zkPath: Option[String] = None
  }


  private val secondsWait = 10

  private val requestTimeoutMs = 500

  override def beforeAll(): Unit = {
    zkServer
    zkClient
  }

  override def afterAll(): Unit = {
    zkClient.close()
    zkServer.close()
  }

//  "Client" should "send request with such ttl that it will never converge to a stable state due to the pipeline." in {
//    val port = util.Utils.getRandomPort
//
//    val serverGotRequest = new AtomicInteger(0)
//    val nettyServer = new NettyServer(
//      host,
//      port,
//      (ch: SocketChannel) => {
//        ch.pipeline()
//          .addLast(
//            new ChannelInboundHandlerAdapter {
//              override def channelRead(ctx: ChannelHandlerContext,
//                                       msg: scala.Any): Unit = {
//                serverGotRequest.getAndIncrement()
//              }
//            })
//      }
//    )
//
//    val task = new Thread {
//      override def run(): Unit = {
//        nettyServer.start()
//      }
//    }
//    task.start()
//
//    val socket = SocketHostPortPair
//      .validateAndCreate(host, port)
//      .get
//
//    val masterPrefix = s"/$uuid"
//    val masterElectionPrefix = s"/$uuid"
//    val zKMasterElector = new ZKMasterElector(
//      zkClient,
//      socket,
//      masterPrefix,
//      masterElectionPrefix
//    )
//    zKMasterElector.start()
//
//
//    val authOpts: AuthOptions =
//      ClientOptions.AuthOptions()
//    val address =
//      zkServer.getConnectString
//    val zookeeperOpts: ZookeeperOptions =
//      CommonOptions.ZookeeperOptions(
//        endpoints = address
//      )
//
//
//    val retryDelayMsForThat = 100
//    val retryCount = 10
//    val connectionOpts: ConnectionOptions =
//      com.bwsw.tstreamstransactionserver.options.ClientOptions.ConnectionOptions(
//        requestTimeoutMs = requestTimeoutMs,
//        retryDelayMs = retryDelayMsForThat,
//        connectionTimeoutMs = 1000,
//        requestTimeoutRetryCount = retryCount,
//        prefix = masterPrefix
//      )
//
//    val clientTimeoutRequestCounter = new AtomicInteger(0)
//    val client = new Client(connectionOpts, authOpts, zookeeperOpts) {
//      // invoked on response
//      override def onRequestTimeout(): Unit = {
//        clientTimeoutRequestCounter.getAndIncrement()
//      }
//    }
//
//    val stream = getRandomStream
//
//    scala.util.Try(
//      Await.ready(client.putStream(stream), secondsWait.seconds)
//    )
//
//    val serverRequestCounter = serverGotRequest
//      .get().toDouble
//    val clientRequestCounter = clientTimeoutRequestCounter
//      .get().toDouble
//
//    client.shutdown()
//    zKMasterElector.stop()
//    nettyServer.shutdown()
//    task.interrupt()
//
//    val error = (serverRequestCounter / 100.0) * 25.0
//    val leftBound = serverRequestCounter - error
//    val rightBound = serverRequestCounter
//
//    clientRequestCounter should be >= leftBound
//    clientRequestCounter should be <= rightBound
//  }

  it should "throw an user defined exception on overriding onRequestTimeout method" in {
    val port = util.Utils.getRandomPort

    val serverGotRequest = new AtomicInteger(0)
    val nettyServer = new NettyServer(
      host,
      port,
      (ch: SocketChannel) => {
        ch.pipeline()
          .addLast(
            new ChannelInboundHandlerAdapter {
              override def channelRead(ctx: ChannelHandlerContext,
                                       msg: scala.Any): Unit = {
                serverGotRequest.getAndIncrement()
              }
            })
      }
    )

    val task = new Thread {
      override def run(): Unit = {
        nettyServer.start()
      }
    }
    task.start()

    val socket = SocketHostPortPair
      .validateAndCreate(host, port)
      .get

    val masterPrefix = s"/$uuid"
    val masterElectionPrefix = s"/$uuid"
    val zKMasterElector = new ZKMasterElector(
      zkClient,
      socket,
      masterPrefix,
      masterElectionPrefix
    )
    zKMasterElector.start()


    val authOpts: AuthOptions =
      ClientOptions.AuthOptions()
    val address =
      zkServer.getConnectString
    val zookeeperOpts: ZookeeperOptions =
      CommonOptions.ZookeeperOptions(
        endpoints = address
      )


    val retryDelayMsForThat = 100
    val retryCount = 10
    val connectionOpts: ConnectionOptions =
      com.bwsw.tstreamstransactionserver.options.ClientOptions.ConnectionOptions(
        requestTimeoutMs = requestTimeoutMs,
        retryDelayMs = retryDelayMsForThat,
        connectionTimeoutMs = 1000,
        requestTimeoutRetryCount = retryCount,
        prefix = masterPrefix
      )

    class MyThrowable extends Exception("My exception")

    val client = new Client(connectionOpts, authOpts, zookeeperOpts) {
      override def onRequestTimeout(): Unit = throw new MyThrowable
    }

    val stream = getRandomStream

    assertThrows[MyThrowable] {
      Await.result(client.putStream(stream), secondsWait.seconds)
    }

    client.shutdown()
    client.shutdown()
    zKMasterElector.stop()
    nettyServer.shutdown()
    task.interrupt()
  }


  it should "throw an user defined exception on overriding onServerConnectionLost method" in {
    val masterPrefix = s"/$uuid"
    val masterElectionPrefix = s"/$uuid"

    val address =
      zkServer.getConnectString

    val authOpts: AuthOptions =
      ClientOptions.AuthOptions()

    val zookeeperOpts: ZookeeperOptions =
      CommonOptions.ZookeeperOptions(
        endpoints = address
      )
    val connectionOpts: ConnectionOptions =
      com.bwsw.tstreamstransactionserver.options.ClientOptions.ConnectionOptions(
        requestTimeoutMs = requestTimeoutMs,
        connectionTimeoutMs = 100,
        prefix = masterPrefix
      )

    val port = Utils.getRandomPort
    val socket = SocketHostPortPair
      .validateAndCreate("127.0.0.1", port)
      .get

    val zKLeaderClientToPutMaster = new ZookeeperClient(
      endpoints = zkServer.getConnectString,
      zookeeperOpts.sessionTimeoutMs,
      zookeeperOpts.connectionTimeoutMs,
      new RetryForever(zookeeperOpts.retryDelayMs)
    )

    val masterElector =
      zKLeaderClientToPutMaster
        .masterElector(
          socket,
          connectionOpts.prefix,
          masterElectionPrefix
        )


    masterElector.start()

    val promise = Promise[Unit]()
    class MyThrowable extends Exception("My exception")
    val client = new Client(connectionOpts, authOpts, zookeeperOpts) {
      override def onServerConnectionLost(): Unit = {
        promise.tryFailure(new MyThrowable)
      }
    }

    assertThrows[MyThrowable] {
      Await.result(promise.future, 5.seconds)
    }

    client.shutdown()
    masterElector.stop()
    zKLeaderClientToPutMaster.close()
  }
}
