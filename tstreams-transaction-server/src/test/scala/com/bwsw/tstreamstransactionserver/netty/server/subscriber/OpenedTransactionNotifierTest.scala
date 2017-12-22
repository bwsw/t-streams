package com.bwsw.tstreamstransactionserver.netty.server.subscriber

import com.bwsw.tstreamstransactionserver.netty.server.db.zk.ZookeeperStreamRepository
import com.bwsw.tstreamstransactionserver.netty.server.streamService.StreamValue
import com.bwsw.tstreamstransactionserver.protocol.TransactionState
import org.scalatest.{FlatSpec, Matchers}
import util.{SubscriberUtils, UdpServer, Utils}

class OpenedTransactionNotifierTest
  extends FlatSpec
    with Matchers
{

  "Open transaction state notifier" should "transmit message to its subscriber" in {
    val (zkServer, zkClient) = Utils.startZkServerAndGetIt
    val zookeeperStreamRepository = new ZookeeperStreamRepository(zkClient, "/tts")
    val timeToUpdateMs = 200

    val observer = new SubscribersObserver(
      zkClient,
      zookeeperStreamRepository,
      timeToUpdateMs
    )
    val subscriberNotifier = new SubscriberNotifier
    val notifier = new OpenedTransactionNotifier(observer, subscriberNotifier)

    val streamBody = StreamValue(0.toString, 100, None, 1000L, None)
    val streamKey   = zookeeperStreamRepository.put(streamBody)
    val streamRecord = zookeeperStreamRepository.get(streamKey).get
    val partition  = 1

    val subscriber = new UdpServer
    SubscriberUtils.putSubscriberInStream(
      zkClient,
      streamRecord.zkPath,
      partition,
      subscriber.getSocketAddress
    )

    observer
      .addSteamPartition(streamKey.id, partition)

    val transactionID = 1L
    val count = -1
    val status = TransactionState.Status.Opened
    val ttlMs = 120L
    val authKey = ""
    val isNotReliable = false
    notifier.notifySubscribers(
      streamRecord.id,
      partition,
      transactionID,
      count,
      status,
      ttlMs,
      authKey,
      isNotReliable
    )

    val data = subscriber.recieve(timeout = 0)

    subscriber.close()
    observer.shutdown()
    subscriberNotifier.close()
    zkClient.close()
    zkServer.close()

    val transactionState = TransactionState.parseFrom(data)
    transactionState.transactionID shouldBe transactionID
    transactionState.ttlMs shouldBe ttlMs
    transactionState.authKey shouldBe authKey
    transactionState.isNotReliable shouldBe isNotReliable
  }

  it should "not transmit message to its subscriber as stream doesn't exist" in {
    val (zkServer, zkClient) = Utils.startZkServerAndGetIt
    val zookeeperStreamRepository = new ZookeeperStreamRepository(zkClient, "/tts")
    val timeToUpdateMs = 200

    val observer = new SubscribersObserver(
      zkClient,
      zookeeperStreamRepository,
      timeToUpdateMs
    )
    val subscriberNotifier = new SubscriberNotifier
    val notifier = new OpenedTransactionNotifier(observer, subscriberNotifier)

    val streamBody = StreamValue(0.toString, 100, None, 1000L, None)
    val streamKey   = zookeeperStreamRepository.put(streamBody)
    val streamRecord = zookeeperStreamRepository.get(streamKey).get
    val partition  = 1

    val subscriber = new UdpServer
    SubscriberUtils.putSubscriberInStream(
      zkClient,
      streamRecord.zkPath,
      partition,
      subscriber.getSocketAddress
    )

    observer
      .addSteamPartition(streamKey.id, partition)

    val fakeStreamID = -200
    val transactionID = 1L
    val count = -1
    val status = TransactionState.Status.Opened
    val ttlMs = 120L
    val authKey = ""
    val isNotReliable = false
    notifier.notifySubscribers(
      fakeStreamID,
      partition,
      transactionID,
      count,
      status,
      ttlMs,
      authKey,
      isNotReliable
    )

    assertThrows[java.net.SocketTimeoutException] {
      subscriber.recieve(timeout = 3000)
    }

    subscriber.close()
    observer.shutdown()
    subscriberNotifier.close()
    zkClient.close()
    zkServer.close()
  }

  it should "transmit message to its subscribers and they will get the same message" in {
    val (zkServer, zkClient) = Utils.startZkServerAndGetIt
    val zookeeperStreamRepository = new ZookeeperStreamRepository(zkClient, "/tts")
    val timeToUpdateMs = 200

    val observer = new SubscribersObserver(
      zkClient,
      zookeeperStreamRepository,
      timeToUpdateMs
    )
    val subscriberNotifier = new SubscriberNotifier
    val notifier = new OpenedTransactionNotifier(observer, subscriberNotifier)

    val streamBody = StreamValue(0.toString, 100, None, 1000L, None)
    val streamKey   = zookeeperStreamRepository.put(streamBody)
    val streamRecord = zookeeperStreamRepository.get(streamKey).get
    val partition  = 1

    val subscribersNum = 10
    val subscribers = Array.fill(subscribersNum){
      val subscriber = new UdpServer
      SubscriberUtils.putSubscriberInStream(
        zkClient,
        streamRecord.zkPath,
        partition,
        subscriber.getSocketAddress
      )
      subscriber
    }

    observer
      .addSteamPartition(streamKey.id, partition)

    val transactionID = 1L
    val count = -1
    val status = TransactionState.Status.Opened
    val ttlMs = 120L
    val authKey = ""
    val isNotReliable = false
    notifier.notifySubscribers(
      streamRecord.id,
      partition,
      transactionID,
      count,
      status,
      ttlMs,
      authKey,
      isNotReliable
    )

    val data = subscribers.map(subscriber =>
      TransactionState.parseFrom(subscriber.recieve(0))
    )

    subscribers.foreach(subscriber => subscriber.close())
    observer.shutdown()
    subscriberNotifier.close()
    zkClient.close()
    zkServer.close()

    data.distinct.length shouldBe 1
  }

}
