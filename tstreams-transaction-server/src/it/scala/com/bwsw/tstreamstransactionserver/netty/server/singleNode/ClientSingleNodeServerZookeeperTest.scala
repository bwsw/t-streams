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

package com.bwsw.tstreamstransactionserver.netty.server.singleNode

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreamstransactionserver.exception.Throwable._
import com.bwsw.tstreamstransactionserver.netty.SocketHostPortPair
import com.bwsw.tstreamstransactionserver.netty.client.zk.ZKMasterPathMonitor
import com.bwsw.tstreamstransactionserver.netty.client.{ClientBuilder, MasterReelectionListener}
import com.bwsw.tstreamstransactionserver.netty.server.zk.ZookeeperClient
import com.bwsw.tstreamstransactionserver.options.ClientOptions.ConnectionOptions
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.{BootstrapOptions, StorageOptions}
import com.bwsw.tstreamstransactionserver.util
import com.bwsw.tstreamstransactionserver.util.Utils
import com.bwsw.tstreamstransactionserver.util.Utils.{getRandomConsumerTransaction, getRandomProducerTransaction, getRandomStream, startZookeeperServer, _}
import com.bwsw.tstreamstransactionserver.util.multiNode.MultiNodeUtils
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.RetryForever
import org.apache.curator.test.TestingServer
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.ZooDefs.{Ids, Perms}
import org.apache.zookeeper.data.ACL
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._


class ClientSingleNodeServerZookeeperTest
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll {
  private val zkTestServer = new TestingServer(false)
  private val secondsWait = 10


  override def beforeAll(): Unit = {
    zkTestServer.start()
  }

  override def afterAll(): Unit = {
    zkTestServer.stop()
    zkTestServer.close()
  }


  "Client" should "not connect to zookeeper server that isn't running" in {
    val clientBuilder = new ClientBuilder()
      .withZookeeperOptions(
        ZookeeperOptions(
          endpoints = "127.0.0.1:8888",
          connectionTimeoutMs = 2000
        )
      )

    assertThrows[ZkNoConnectionException] {
      clientBuilder.build()
    }
  }


  it should "not connect to server which socket address(retrieved from zooKeeper server) is wrong" in {
    val zkPrefix = s"/$uuid"

    val zkClient = CuratorFrameworkFactory.builder()
      .sessionTimeoutMs(1000)
      .connectionTimeoutMs(1000)
      .retryPolicy(new RetryForever(100))
      .connectString(zkTestServer.getConnectString)
      .build()
    zkClient.start()
    zkClient.blockUntilConnected(1, TimeUnit.SECONDS)

    val permissions = new java.util.ArrayList[ACL]()
    permissions.add(new ACL(Perms.READ, Ids.ANYONE_ID_UNSAFE))
    zkClient.create().creatingParentsIfNeeded()
      .withMode(CreateMode.EPHEMERAL)
      .withACL(permissions)
      .forPath(zkPrefix, "Test".getBytes())

    val clientBuilder = new ClientBuilder()
      .withConnectionOptions(ConnectionOptions(prefix = zkPrefix))
      .withZookeeperOptions(
        ZookeeperOptions(
          endpoints = zkTestServer.getConnectString
        )
      )

    assertThrows[MasterDataIsIllegalException] {
      clientBuilder.build()
    }

    zkClient.close()
  }

  it should "not connect to server which socket address(retrieved from zooKeeper server) is putted on persistent znode" in {
    val zkPrefix = s"/$uuid"

    val zkClient = CuratorFrameworkFactory.builder()
      .sessionTimeoutMs(1000)
      .connectionTimeoutMs(1000)
      .retryPolicy(new RetryForever(100))
      .connectString(zkTestServer.getConnectString)
      .build()
    zkClient.start()
    zkClient.blockUntilConnected(1, TimeUnit.SECONDS)

    val permissions = new java.util.ArrayList[ACL]()
    permissions.add(new ACL(Perms.READ, Ids.ANYONE_ID_UNSAFE))
    zkClient.create().creatingParentsIfNeeded()
      .withMode(CreateMode.PERSISTENT)
      .withACL(permissions)
      .forPath(zkPrefix, "Test".getBytes())

    val clientBuilder = new ClientBuilder()
      .withConnectionOptions(ConnectionOptions(prefix = zkPrefix))
      .withZookeeperOptions(
        ZookeeperOptions(
          endpoints = zkTestServer.getConnectString
        )
      )

    assertThrows[MasterIsPersistentZnodeException] {
      clientBuilder.build()
    }

    zkClient.close()
  }


  it should "not connect to server which inet address(retrieved from zooKeeper server) is wrong" in {
    val zkPrefix = s"/$uuid"

    val zkClient = CuratorFrameworkFactory.builder()
      .sessionTimeoutMs(1000)
      .connectionTimeoutMs(1000)
      .retryPolicy(new RetryForever(100))
      .connectString(zkTestServer.getConnectString)
      .build()
    zkClient.start()
    zkClient.blockUntilConnected(1, TimeUnit.SECONDS)

    val permissions = new java.util.ArrayList[ACL]()
    permissions.add(new ACL(Perms.READ, Ids.ANYONE_ID_UNSAFE))
    zkClient.create().creatingParentsIfNeeded()
      .withMode(CreateMode.EPHEMERAL)
      .withACL(permissions)
      .forPath(zkPrefix, "1270.0.0.1:8080".getBytes())

    val clientBuilder = new ClientBuilder()
      .withConnectionOptions(ConnectionOptions(prefix = zkPrefix))
      .withZookeeperOptions(
        ZookeeperOptions(
          endpoints = zkTestServer.getConnectString
        )
      )

    assertThrows[MasterDataIsIllegalException] {
      clientBuilder.build()
    }

    zkClient.close()
  }

  it should "not connect to server which port value(retrieved from zooKeeper server) is negative" in {
    val zkPrefix = s"/$uuid"

    val zkClient = CuratorFrameworkFactory.builder()
      .sessionTimeoutMs(1000)
      .connectionTimeoutMs(1000)
      .retryPolicy(new RetryForever(100))
      .connectString(zkTestServer.getConnectString)
      .build()
    zkClient.start()
    zkClient.blockUntilConnected(1, TimeUnit.SECONDS)

    val permissions = new java.util.ArrayList[ACL]()
    permissions.add(new ACL(Perms.READ, Ids.ANYONE_ID_UNSAFE))
    zkClient.create().creatingParentsIfNeeded()
      .withMode(CreateMode.EPHEMERAL)
      .withACL(permissions)
      .forPath(zkPrefix, "1270.0.0.1:-8080".getBytes())

    val clientBuilder = new ClientBuilder()
      .withConnectionOptions(ConnectionOptions(prefix = zkPrefix))
      .withZookeeperOptions(
        ZookeeperOptions(
          endpoints = zkTestServer.getConnectString
        )
      )

    assertThrows[MasterDataIsIllegalException] {
      clientBuilder.build()
    }

    zkClient.close()
  }

  it should "not connect to server which port value(retrieved from zooKeeper server) exceeds 65535" in {
    val zkPrefix = s"/$uuid"

    val zkClient = CuratorFrameworkFactory.builder()
      .sessionTimeoutMs(1000)
      .connectionTimeoutMs(1000)
      .retryPolicy(new RetryForever(100))
      .connectString(zkTestServer.getConnectString)
      .build()
    zkClient.start()
    zkClient.blockUntilConnected(1, TimeUnit.SECONDS)

    val permissions = new java.util.ArrayList[ACL]()
    permissions.add(new ACL(Perms.READ, Ids.ANYONE_ID_UNSAFE))
    zkClient.create().creatingParentsIfNeeded()
      .withMode(CreateMode.EPHEMERAL)
      .withACL(permissions)
      .forPath(zkPrefix, "1270.0.0.1:65536".getBytes())

    val clientBuilder = new ClientBuilder()
      .withConnectionOptions(ConnectionOptions(prefix = zkPrefix))
      .withZookeeperOptions(
        ZookeeperOptions(
          endpoints = zkTestServer.getConnectString
        )
      )

    assertThrows[MasterDataIsIllegalException] {
      clientBuilder.build()
    }

    zkClient.close()
  }

  it should "monitor master prefix changes, and when the server shutdown, starts on another port — client should reconnect properly" in {
    val zkPrefix = s"/$uuid/master"
    val masterElectionPrefix = s"/$uuid/master_election"
    val zkOptions = ZookeeperOptions(
      endpoints = zkTestServer.getConnectString
    )

    val zk = new ZookeeperClient(
      zkOptions.endpoints,
      zkOptions.sessionTimeoutMs,
      zkOptions.connectionTimeoutMs,
      new RetryForever(zkOptions.retryDelayMs)
    )

    val host = "127.0.0.1"
    val port1 = util.Utils.getRandomPort
    val address1 = SocketHostPortPair
      .fromString(s"$host:$port1")
      .get
    val port2 = util.Utils.getRandomPort
    val address2 = SocketHostPortPair
      .fromString(s"$host:$port2")
      .get

    val elector1 = zk.masterElector(
      address1,
      zkPrefix,
      masterElectionPrefix
    )

    val elector2 = zk.masterElector(
      address2,
      zkPrefix,
      masterElectionPrefix
    )

    val zkClient = CuratorFrameworkFactory.builder()
      .sessionTimeoutMs(1000)
      .connectionTimeoutMs(1000)
      .retryPolicy(new RetryForever(100))
      .connectString(zkTestServer.getConnectString)
      .build()
    zkClient.start()
    zkClient.blockUntilConnected(1, TimeUnit.SECONDS)


    val latch = new CountDownLatch(2)
    val zKMasterPathMonitor = new
        ZKMasterPathMonitor(
          zkClient,
          200,
          zkPrefix
        )

    val listener = new MasterReelectionListener {
      override def masterChanged(newMaster: Either[Throwable, Option[SocketHostPortPair]]): Unit = {
        if (latch.getCount == 2) {
          newMaster.right.get.get shouldBe address1
          elector1.stop()
          latch.countDown()
        }
        else
          newMaster.right.get.get shouldBe address2
        latch.countDown()
      }
    }
    zKMasterPathMonitor.addMasterReelectionListener(listener)
    zKMasterPathMonitor.startMonitoringMasterServerPath()


    elector1.start()
    Thread.sleep(100)
    elector2.start()

    latch.await(5000, TimeUnit.MILLISECONDS) shouldBe true

    elector2.stop()
    zkClient.close()
  }

  it should "throw a ZkNoConnectionException when client lost connection with ZooKeeper" in {
    val connectionTimeoutMs = 1000
    val sessionTimeoutMs = 10000
    val (zkServer, zkClient) = startZookeeperServer
    val serverBuilder = new SingleNodeServerBuilder()
      .withCommitLogOptions(SingleNodeServerOptions.CommitLogOptions(closeDelayMs = Int.MaxValue))

    val clientBuilder = new ClientBuilder()
      .withZookeeperOptions(ZookeeperOptions(
        sessionTimeoutMs = sessionTimeoutMs,
        connectionTimeoutMs = connectionTimeoutMs))

    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder)

    bundle.operate { _ =>
      val client = bundle.client
      val stream = getRandomStream
      zkServer.close()
      Thread.sleep(sessionTimeoutMs) // wait until ZooKeeper session expires

      val producerTransactions = Array.fill(100)(getRandomProducerTransaction(1, stream))
      val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(1, stream))

      a[ZkNoConnectionException] shouldBe thrownBy {
        Await.result(
          client.putTransactions(producerTransactions, consumerTransactions),
          secondsWait.seconds)
      }
    }
  }


  "Server" should "not connect to zookeeper server that isn't running" in {
    val storageOptions = StorageOptions()
    val port = util.Utils.getRandomPort
    val serverBuilder = new SingleNodeServerBuilder()
      .withServerStorageOptions(storageOptions)
      .withZookeeperOptions(ZookeeperOptions(endpoints = s"127.0.0.1:$port", connectionTimeoutMs = 2000))

    assertThrows[ZkNoConnectionException] {
      serverBuilder.build()
    }

    MultiNodeUtils.deleteDirectories(storageOptions)
  }

  it should "not start on wrong inet address" in {
    val storageOptions = StorageOptions()
    val serverBuilder = new SingleNodeServerBuilder()
      .withServerStorageOptions(storageOptions)
      .withZookeeperOptions(ZookeeperOptions(endpoints = zkTestServer.getConnectString))
      .withBootstrapOptions(BootstrapOptions(bindHost = "1270.0.0.1"))

    assertThrows[InvalidSocketAddress] {
      serverBuilder.build()
    }

    MultiNodeUtils.deleteDirectories(storageOptions)
  }

  it should "not start on negative port value" in {
    val storageOptions = StorageOptions()
    val serverBuilder = new SingleNodeServerBuilder()
      .withServerStorageOptions(storageOptions)
      .withZookeeperOptions(ZookeeperOptions(endpoints = zkTestServer.getConnectString))
      .withBootstrapOptions(BootstrapOptions(bindPort = Int.MinValue))

    assertThrows[InvalidSocketAddress] {
      serverBuilder.build()
    }

    MultiNodeUtils.deleteDirectories(storageOptions)
  }

  it should "not start on port value exceeds 65535" in {
    val storageOptions = StorageOptions()
    val serverBuilder = new SingleNodeServerBuilder()
      .withServerStorageOptions(storageOptions)
      .withZookeeperOptions(ZookeeperOptions(endpoints = zkTestServer.getConnectString))
      .withBootstrapOptions(BootstrapOptions(bindPort = 65536))

    assertThrows[InvalidSocketAddress] {
      serverBuilder.build()
    }

    MultiNodeUtils.deleteDirectories(storageOptions)
  }

}
