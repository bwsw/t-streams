//package it.replication
//
//import com.bwsw.tstreamstransactionserver.netty.server.ZKClientServer
//import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
//import org.apache.curator.framework.CuratorFramework
//import org.apache.curator.framework.recipes.cache.{PathChildrenCacheEvent, PathChildrenCacheListener}
//import org.apache.curator.framework.recipes.leader.LeaderLatchListener
//import org.apache.curator.retry.RetryForever
//import org.apache.curator.test.TestingServer
//import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
//
//import scala.collection.mutable
//
//class ServerZkClientTest extends FlatSpec with Matchers with BeforeAndAfterEach {
//  private val replicationCoordinationParh = "/test_group"
//
//  "Replication group" should "have one participant and as consequence an one master" in {
//    val zkTestServer = new TestingServer(true)
//    val zookeeperOpts: ZookeeperOptions = com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions(endpoints = zkTestServer.getConnectString)
//
//    val address = "127.0.0.1"
//    val port = 1000
//
//    val zKClient = new ZKClientServer(
//      address,
//      port,
//      endpoints = zkTestServer.getConnectString,
//      zookeeperOpts.sessionTimeoutMs,
//      zookeeperOpts.connectionTimeoutMs,
//      new RetryForever(zookeeperOpts.retryDelayMs)
//    )
//
//    val replicationGroup = zKClient.replicationGroup(replicationCoordinationParh)
//
//    var isLeaderGroup = false
//    val election = replicationGroup.election(new LeaderLatchListener {
//      override def isLeader(): Unit = isLeaderGroup = true
//      override def notLeader(): Unit = isLeaderGroup = false
//    })
//    election.join()
//
//    Thread.sleep(100)
//
//    s"$address:$port" shouldBe election.participants.head.getId
//    isLeaderGroup shouldBe true
//    election.leave()
//    zKClient.close()
//    zkTestServer.close()
//  }
//
//  it should "have 2 participants, the first is master, the second one is slave" in {
//    val zkTestServer = new TestingServer(true)
//    val zookeeperOpts: ZookeeperOptions = com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions(endpoints = zkTestServer.getConnectString)
//
//    val address1 = "127.0.0.1"
//    val port1 = 1000
//
//    val address2 = "127.0.0.4"
//    val port2 = 1430
//
//    val zKClient1 = new ZKClientServer(
//      address1,
//      port1,
//      endpoints = zkTestServer.getConnectString,
//      zookeeperOpts.sessionTimeoutMs,
//      zookeeperOpts.connectionTimeoutMs,
//      new RetryForever(zookeeperOpts.retryDelayMs)
//    )
//
//    val zKClient2 = new ZKClientServer(
//      address2,
//      port2,
//      endpoints = zkTestServer.getConnectString,
//      zookeeperOpts.sessionTimeoutMs,
//      zookeeperOpts.connectionTimeoutMs,
//      new RetryForever(zookeeperOpts.retryDelayMs)
//    )
//
//    val replicationGroup1 = zKClient1.replicationGroup(replicationCoordinationParh)
//    val replicationGroup2 = zKClient2.replicationGroup(replicationCoordinationParh)
//
//    var isLeaderGroup1 = false
//    val election1 = replicationGroup1.election(new LeaderLatchListener {
//      override def isLeader(): Unit = isLeaderGroup1 = true
//      override def notLeader(): Unit = isLeaderGroup1 = false
//    })
//    election1.join()
//
//    Thread.sleep(100)
//
//    var isLeaderGroup2 = false
//    val election2 = replicationGroup2.election(new LeaderLatchListener {
//      override def isLeader(): Unit = isLeaderGroup2 = true
//      override def notLeader(): Unit = isLeaderGroup2 = false
//    })
//    election2.join()
//
//    Thread.sleep(100)
//
//    election1.participants should contain theSameElementsAs election2.participants
//
//    isLeaderGroup1 shouldBe true
//    isLeaderGroup2 shouldBe false
//
//    election1.leave()
//    election2.leave()
//    zKClient1.close()
//    zKClient2.close()
//    zkTestServer.close()
//  }
//
//  it should "have 2 participants, the second one is master due to the first leave election" in {
//    val zkTestServer = new TestingServer(true)
//    val zookeeperOpts: ZookeeperOptions = com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions(endpoints = zkTestServer.getConnectString)
//
//    val address1 = "127.0.0.1"
//    val port1 = 1000
//
//    val address2 = "127.0.0.4"
//    val port2 = 1430
//
//    val zKClient1 = new ZKClientServer(
//      address1,
//      port1,
//      endpoints = zkTestServer.getConnectString,
//      zookeeperOpts.sessionTimeoutMs,
//      zookeeperOpts.connectionTimeoutMs,
//      new RetryForever(zookeeperOpts.retryDelayMs)
//    )
//
//    val zKClient2 = new ZKClientServer(
//      address2,
//      port2,
//      endpoints = zkTestServer.getConnectString,
//      zookeeperOpts.sessionTimeoutMs,
//      zookeeperOpts.connectionTimeoutMs,
//      new RetryForever(zookeeperOpts.retryDelayMs)
//    )
//
//    val replicationGroup1 = zKClient1.replicationGroup(replicationCoordinationParh)
//    val replicationGroup2 = zKClient2.replicationGroup(replicationCoordinationParh)
//
//    var isLeaderGroup1 = false
//    val election1 = replicationGroup1.election(new LeaderLatchListener {
//      override def isLeader(): Unit = isLeaderGroup1 = true
//      override def notLeader(): Unit = isLeaderGroup1 = false
//    })
//    election1.join()
//
//    Thread.sleep(100)
//
//    var isLeaderGroup2 = false
//    val election2 = replicationGroup2.election(new LeaderLatchListener {
//      override def isLeader(): Unit = isLeaderGroup2 = true
//      override def notLeader(): Unit = isLeaderGroup2 = false
//    })
//    election2.join()
//
//    Thread.sleep(50)
//
//    election1.participants should contain theSameElementsAs election2.participants
//
//    isLeaderGroup1 shouldBe true
//    isLeaderGroup2 shouldBe false
//
//    election1.leave()
//
//    Thread.sleep(50)
//
//    isLeaderGroup1 shouldBe false
//    isLeaderGroup2 shouldBe true
//
//    election1.participants should contain theSameElementsAs election2.participants
//
//    election2.leave()
//    zKClient1.close()
//    zKClient2.close()
//    zkTestServer.close()
//  }
//
//  it should "have 4 participants, the first one know about new participants via listener" in {
//    val zkTestServer = new TestingServer(true)
//    val zookeeperOpts: ZookeeperOptions = com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions(endpoints = zkTestServer.getConnectString)
//
//    val address1 = "127.0.0.1"
//    val port1 = 1000
//
//    val address2 = "127.0.0.2"
//    val port2 = 1100
//
//    val address3 = "127.0.0.3"
//    val port3 = 1200
//
//    val address4 = "127.0.0.4"
//    val port4 = 1300
//
//
//    val zKClient1 = new ZKClientServer(
//      address1,
//      port1,
//      endpoints = zkTestServer.getConnectString,
//      zookeeperOpts.sessionTimeoutMs,
//      zookeeperOpts.connectionTimeoutMs,
//      new RetryForever(zookeeperOpts.retryDelayMs)
//    )
//
//    val zKClient2 = new ZKClientServer(
//      address2,
//      port2,
//      endpoints = zkTestServer.getConnectString,
//      zookeeperOpts.sessionTimeoutMs,
//      zookeeperOpts.connectionTimeoutMs,
//      new RetryForever(zookeeperOpts.retryDelayMs)
//    )
//
//    val zKClient3 = new ZKClientServer(
//      address3,
//      port3,
//      endpoints = zkTestServer.getConnectString,
//      zookeeperOpts.sessionTimeoutMs,
//      zookeeperOpts.connectionTimeoutMs,
//      new RetryForever(zookeeperOpts.retryDelayMs)
//    )
//
//    val zKClient4 = new ZKClientServer(
//      address4,
//      port4,
//      endpoints = zkTestServer.getConnectString,
//      zookeeperOpts.sessionTimeoutMs,
//      zookeeperOpts.connectionTimeoutMs,
//      new RetryForever(zookeeperOpts.retryDelayMs)
//    )
//
//    val replicationGroup1 = zKClient1.replicationGroup(replicationCoordinationParh)
//    val replicationGroup2 = zKClient2.replicationGroup(replicationCoordinationParh)
//    val replicationGroup3 = zKClient3.replicationGroup(replicationCoordinationParh)
//    val replicationGroup4 = zKClient4.replicationGroup(replicationCoordinationParh)
//
//    import PathChildrenCacheEvent.Type._
//    val children = new collection.mutable.ArrayBuffer[String]()
//
//    var childAddedCounter = 0
//    var childRemovedCounter = 0
//    var otherEvenstsCounter = 0
//    val listener1 = replicationGroup1.listener((_: CuratorFramework, event: PathChildrenCacheEvent) => event.getType match {
//      case CHILD_ADDED =>
//        val socketAddress = new String(event.getData.getData)
//        children += socketAddress
//        childAddedCounter = childAddedCounter + 1
//      case CHILD_REMOVED =>
//        childRemovedCounter = childRemovedCounter + 1
//      case _ =>
//        otherEvenstsCounter = otherEvenstsCounter + 1
//    })
//
//
//    var isLeaderGroup1 = false
//    val election1 = replicationGroup1.election(new LeaderLatchListener {
//      override def isLeader(): Unit = isLeaderGroup1 = true
//      override def notLeader(): Unit = isLeaderGroup1 = false
//    })
//
//    election1.join()
//    Thread.sleep(100)
//
//    var isLeaderGroup2 = false
//    val election2 = replicationGroup2.election(new LeaderLatchListener {
//      override def isLeader(): Unit = isLeaderGroup2 = true
//      override def notLeader(): Unit = isLeaderGroup2 = false
//    })
//
//    election2.join()
//    Thread.sleep(50)
//
//    var isLeaderGroup3 = false
//    val election3 = replicationGroup3.election(new LeaderLatchListener {
//      override def isLeader(): Unit = isLeaderGroup3 = true
//      override def notLeader(): Unit = isLeaderGroup3 = false
//    })
//
//    election3.join()
//    Thread.sleep(50)
//
//    var isLeaderGroup4 = false
//    val election4 = replicationGroup4.election(new LeaderLatchListener {
//      override def isLeader(): Unit = isLeaderGroup4 = true
//      override def notLeader(): Unit = isLeaderGroup4 = false
//    })
//
//    election4.join()
//    Thread.sleep(50)
//    childAddedCounter shouldBe 4
//    otherEvenstsCounter shouldBe 0
//
//    election1.participants.map(participant => participant.getId) should contain theSameElementsAs children
//
//    election1.leave()
//    Thread.sleep(50)
//    childRemovedCounter shouldBe 1
//
//    election2.leave()
//    Thread.sleep(50)
//    childRemovedCounter shouldBe 2
//
//    election3.leave()
//    election4.leave()
//
//    listener1.close()
//
//    zKClient1.close()
//    zKClient2.close()
//    zKClient3.close()
//    zKClient4.close()
//    zkTestServer.close()
//  }
//}
