//package it.client
//
//import java.util.concurrent.{CountDownLatch, TimeUnit}
//import java.util.concurrent.atomic.AtomicInteger
//
//import com.bwsw.tstreamstransactionserver.netty.SocketHostPortPair
//import com.bwsw.tstreamstransactionserver.netty.client.NettyConnection
//import io.netty.channel._
//import io.netty.channel.epoll.EpollEventLoopGroup
//import io.netty.channel.nio.NioEventLoopGroup
//import io.netty.handler.codec.bytes.ByteArrayEncoder
//import org.apache.commons.lang.SystemUtils
//import org.scalatest.{FlatSpec, Matchers}
//import util.Utils
//import util.netty.NettyServerHandler
//
//class NettyConnectionTest
//  extends FlatSpec
//    with Matchers {
//
//  private def handlersChain =
//    Seq(
//      new ByteArrayEncoder(),
//      new NettyServerHandler()
//    )
//
//  private def getClient(workerGroup: EventLoopGroup,
//                        socket: SocketHostPortPair,
//                        onConnectionLostDo: => Unit) = {
//    new NettyConnection(
//      workerGroup,
//      handlersChain,
//      3000,
//      30,
//      socket,
//      onConnectionLostDo
//    )
//  }
//
//  private def createEventLoopGroup(): EventLoopGroup = {
//    if (SystemUtils.IS_OS_LINUX) {
//      new EpollEventLoopGroup()
//    }
//    else {
//      new NioEventLoopGroup()
//    }
//  }
//
//  private def buildSocket = {
//    val host = "127.0.0.1"
//    val port = Utils.getRandomPort
//    SocketHostPortPair(
//      host,
//      port
//    )
//  }
//
//  it should "tries to reconnect to server multiple times." in {
//    val reconnectAttemptsNumber = 5
//    val timePerReconnect = 100
//
//    val socket = buildSocket
//
//    val testServer = new util.netty.NettyServer(
//      socket.address,
//      socket.port
//    )
//    testServer.start()
//
//    val workerGroup: EventLoopGroup =
//      createEventLoopGroup()
//
//    val latch = new CountDownLatch(reconnectAttemptsNumber)
//    getClient(workerGroup, socket, {
//      latch.countDown()
//    })
//
//    testServer.shutdown()
//
//    latch.await(
//      reconnectAttemptsNumber*timePerReconnect*10,
//      TimeUnit.MILLISECONDS
//    ) shouldBe true
//
//    scala.util.Try(
//      workerGroup
//        .shutdownGracefully(0L, 0L, TimeUnit.NANOSECONDS)
//        .awaitUninterruptibly(1000L)
//    )
//  }
//
//  it should "reconnect to server after the while." in {
//    val socket = buildSocket
//
//    val testServer1 = new util.netty.NettyServer(
//      socket.address,
//      socket.port
//    )
//    testServer1.start()
//
//    val workerGroup: EventLoopGroup =
//      createEventLoopGroup()
//
//    val reconnectAttemptsNumber = new AtomicInteger(0)
//    getClient(workerGroup, socket, {
//      reconnectAttemptsNumber.getAndIncrement()
//    })
//
//    testServer1.shutdown()
//
//    val reconnectAttemptsNumber1 =
//      reconnectAttemptsNumber.get()
//
//    val testServer2 = new util.netty.NettyServer(
//      socket.address,
//      socket.port
//    )
//    testServer2.start()
//    testServer2.shutdown()
//
//    while (reconnectAttemptsNumber.get <= reconnectAttemptsNumber1) {}
//
//    val reconnectAttemptsNumber2 =
//      reconnectAttemptsNumber.get
//
//    scala.util.Try(
//      workerGroup
//        .shutdownGracefully(0L, 0L, TimeUnit.NANOSECONDS)
//        .awaitUninterruptibly(1000L)
//    )
//
//    assert(reconnectAttemptsNumber2 > reconnectAttemptsNumber1)
//  }
//
//}
