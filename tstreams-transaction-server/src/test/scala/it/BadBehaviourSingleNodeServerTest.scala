/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package it

import java.util.concurrent.atomic.AtomicInteger

import com.bwsw.tstreamstransactionserver.netty.SocketHostPortPair
import com.bwsw.tstreamstransactionserver.netty.client.Client
import com.bwsw.tstreamstransactionserver.netty.server.zk.{ZKMasterElector, ZookeeperClient}
import com.bwsw.tstreamstransactionserver.options.ClientOptions.{AuthOptions, ConnectionOptions}
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.{ClientOptions, CommonOptions}
import io.netty.channel.socket.SocketChannel
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import org.apache.curator.retry.RetryForever
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import util.Utils.{getRandomPort, startZkServerAndGetIt, uuid}
import util.netty.NettyServer

import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}
import scala.util.Try

class BadBehaviourSingleNodeServerTest
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll {

  private lazy val (zkServer, zkClient) = startZkServerAndGetIt

  private val host = "127.0.0.1"
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

  "Client" should "throw an user defined exception on overriding onRequestTimeout method" in {
    val port = getRandomPort

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
    val connectionOpts: ConnectionOptions =
      com.bwsw.tstreamstransactionserver.options.ClientOptions.ConnectionOptions(
        requestTimeoutMs = requestTimeoutMs,
        retryDelayMs = retryDelayMsForThat,
        connectionTimeoutMs = 1000,
        prefix = masterPrefix
      )

    class MyThrowable extends Exception("My exception")

    val promise = Promise[Unit]()
    Try(
      new Client(connectionOpts, authOpts, zookeeperOpts) {
        override def onRequestTimeout(): Unit =
          promise.failure(new MyThrowable)
      }).map(_.shutdown())
    a[MyThrowable] shouldBe thrownBy {
      Await.result(promise.future, 5.seconds)
    }

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

    val port = getRandomPort
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
    Try(new Client(connectionOpts, authOpts, zookeeperOpts) {
      override def onServerConnectionLost(): Unit = {
        promise.tryFailure(new MyThrowable)
      }
    }).map(_.shutdown())

    a[MyThrowable] shouldBe thrownBy {
      Await.result(promise.future, 5.seconds)
    }

    masterElector.stop()
    zKLeaderClientToPutMaster.close()
  }
}
