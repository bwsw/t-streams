package it

import java.util
import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.exception.Throwable.{InvalidSocketAddress, ZkGetMasterException, ZkNoConnectionException}
import com.bwsw.tstreamstransactionserver.options.{ClientBuilder, ServerBuilder}
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.ServerOptions.BootstrapOptions
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.RetryForever
import org.apache.curator.test.TestingServer
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.ZooDefs.{Ids, Perms}
import org.apache.zookeeper.data.ACL
import org.scalatest.{FlatSpec, Matchers}

class ClientServerZookeeperTest extends FlatSpec with Matchers {

  "Client" should "not connect to zookeeper server that isn't running" in {
    val clientBuilder = new ClientBuilder().withZookeeperOptions(ZookeeperOptions(endpoints = "127.0.0.1:8888"))
    assertThrows[ZkNoConnectionException] {
      clientBuilder.build()
    }
  }

  it should "not connect to server which socket address(retrieved from zooKeeper server) is wrong" in {
    val zkPrefix = "/tts"
    val zkTestServer = new TestingServer(true)

    val zkClient = CuratorFrameworkFactory.builder()
      .sessionTimeoutMs(1000)
      .connectionTimeoutMs(1000)
      .retryPolicy(new RetryForever(100))
      .connectString(zkTestServer.getConnectString)
      .build()
    zkClient.start()
    zkClient.blockUntilConnected(1, TimeUnit.SECONDS)

    val permissions = new util.ArrayList[ACL]()
    permissions.add(new ACL(Perms.READ, Ids.ANYONE_ID_UNSAFE))
    zkClient.create().creatingParentsIfNeeded()
      .withMode(CreateMode.EPHEMERAL)
      .withACL(permissions)
      .forPath(zkPrefix, "Test".getBytes())

    val clientBuilder = new ClientBuilder()
      .withZookeeperOptions(ZookeeperOptions(endpoints = zkTestServer.getConnectString, prefix = zkPrefix))

    assertThrows[ZkGetMasterException ] {
      clientBuilder.build()
    }

    zkTestServer.close()
  }

  it should "not connect to server which inet address(retrieved from zooKeeper server) is wrong" in {
    val zkPrefix = "/tts"
    val zkTestServer = new TestingServer(true)

    val zkClient = CuratorFrameworkFactory.builder()
      .sessionTimeoutMs(1000)
      .connectionTimeoutMs(1000)
      .retryPolicy(new RetryForever(100))
      .connectString(zkTestServer.getConnectString)
      .build()
    zkClient.start()
    zkClient.blockUntilConnected(1, TimeUnit.SECONDS)

    val permissions = new util.ArrayList[ACL]()
    permissions.add(new ACL(Perms.READ, Ids.ANYONE_ID_UNSAFE))
    zkClient.create().creatingParentsIfNeeded()
      .withMode(CreateMode.EPHEMERAL)
      .withACL(permissions)
      .forPath(zkPrefix, "1270.0.0.1:8080".getBytes())

    val clientBuilder = new ClientBuilder()
      .withZookeeperOptions(ZookeeperOptions(endpoints = zkTestServer.getConnectString, prefix = zkPrefix))

    assertThrows[ZkGetMasterException ] {
      clientBuilder.build()
    }

    zkTestServer.close()
  }

  it should "not connect to server which port value(retrieved from zooKeeper server) is negative" in {
    val zkPrefix = "/tts"
    val zkTestServer = new TestingServer(true)

    val zkClient = CuratorFrameworkFactory.builder()
      .sessionTimeoutMs(1000)
      .connectionTimeoutMs(1000)
      .retryPolicy(new RetryForever(100))
      .connectString(zkTestServer.getConnectString)
      .build()
    zkClient.start()
    zkClient.blockUntilConnected(1, TimeUnit.SECONDS)

    val permissions = new util.ArrayList[ACL]()
    permissions.add(new ACL(Perms.READ, Ids.ANYONE_ID_UNSAFE))
    zkClient.create().creatingParentsIfNeeded()
      .withMode(CreateMode.EPHEMERAL)
      .withACL(permissions)
      .forPath(zkPrefix, "1270.0.0.1:-8080".getBytes())

    val clientBuilder = new ClientBuilder()
      .withZookeeperOptions(ZookeeperOptions(endpoints = zkTestServer.getConnectString, prefix = zkPrefix))

    assertThrows[ZkGetMasterException ] {
      clientBuilder.build()
    }

    zkTestServer.close()
  }

  it should "not connect to server which port value(retrieved from zooKeeper server) is exceeds 65535" in {
    val zkPrefix = "/tts"
    val zkTestServer = new TestingServer(true)

    val zkClient = CuratorFrameworkFactory.builder()
      .sessionTimeoutMs(1000)
      .connectionTimeoutMs(1000)
      .retryPolicy(new RetryForever(100))
      .connectString(zkTestServer.getConnectString)
      .build()
    zkClient.start()
    zkClient.blockUntilConnected(1, TimeUnit.SECONDS)

    val permissions = new util.ArrayList[ACL]()
    permissions.add(new ACL(Perms.READ, Ids.ANYONE_ID_UNSAFE))
    zkClient.create().creatingParentsIfNeeded()
      .withMode(CreateMode.EPHEMERAL)
      .withACL(permissions)
      .forPath(zkPrefix, "1270.0.0.1:65536".getBytes())

    val clientBuilder = new ClientBuilder()
      .withZookeeperOptions(ZookeeperOptions(endpoints = zkTestServer.getConnectString, prefix = zkPrefix))

    assertThrows[ZkGetMasterException ] {
      clientBuilder.build()
    }

    zkTestServer.close()
  }

  "Server" should "not connect to zookeeper server that isn't running" in {
    val serverBuilder = new ServerBuilder().withZookeeperOptions(ZookeeperOptions(endpoints = "127.0.0.1:8888"))
    assertThrows[ZkNoConnectionException] {
      serverBuilder.build()
    }
  }

  it should "not start on wrong inet address" in {
    val zkTestServer = new TestingServer(true)
    val serverBuilder = new ServerBuilder()
      .withZookeeperOptions(ZookeeperOptions(endpoints = zkTestServer.getConnectString))
      .withServerOptions(BootstrapOptions(host = "1270.0.0.1"))

    assertThrows[InvalidSocketAddress] {
      serverBuilder.build()
    }
    zkTestServer.close()
  }

  it should "not start on negative port value" in {
    val zkTestServer = new TestingServer(true)
    val serverBuilder = new ServerBuilder()
      .withZookeeperOptions(ZookeeperOptions(endpoints = zkTestServer.getConnectString))
      .withServerOptions(BootstrapOptions(port = Int.MinValue))

    assertThrows[InvalidSocketAddress] {
      serverBuilder.build()
    }
    zkTestServer.close()
  }

  it should "not start on port value exceeds 65535" in {
    val zkTestServer = new TestingServer(true)
    val serverBuilder = new ServerBuilder()
      .withZookeeperOptions(ZookeeperOptions(endpoints = zkTestServer.getConnectString))
      .withServerOptions(BootstrapOptions(port = 65536))

    assertThrows[InvalidSocketAddress] {
      serverBuilder.build()
    }
    zkTestServer.close()
  }

}
