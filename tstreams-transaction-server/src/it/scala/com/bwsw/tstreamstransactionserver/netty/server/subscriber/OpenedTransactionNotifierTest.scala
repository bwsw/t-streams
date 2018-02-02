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

package com.bwsw.tstreamstransactionserver.netty.server.subscriber

import com.bwsw.tstreamstransactionserver.netty.Protocol
import com.bwsw.tstreamstransactionserver.netty.server.db.zk.ZookeeperStreamRepository
import com.bwsw.tstreamstransactionserver.netty.server.streamService.StreamValue
import com.bwsw.tstreamstransactionserver.rpc.{TransactionState, TransactionStates}
import com.bwsw.tstreamstransactionserver.util.{SubscriberUtils, UdpServer, Utils}
import org.scalatest.{FlatSpec, Matchers}

class OpenedTransactionNotifierTest
  extends FlatSpec
    with Matchers {

  "Open transaction state notifier" should "transmit message to its subscriber" in {
    val (zkServer, zkClient) = Utils.startZookeeperServer
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
    val streamKey = zookeeperStreamRepository.put(streamBody)
    val streamRecord = zookeeperStreamRepository.get(streamKey).get
    val partition = 1

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
    val status = TransactionStates.Opened
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

    val data = subscriber.receive(timeout = 0)

    subscriber.close()
    observer.shutdown()
    subscriberNotifier.close()
    zkClient.close()
    zkServer.close()

    val transactionState = Protocol.decode(data, TransactionState)

    transactionState.transactionID shouldBe transactionID
    transactionState.ttlMs shouldBe ttlMs
    transactionState.authKey shouldBe authKey
    transactionState.isNotReliable shouldBe isNotReliable
  }

  it should "not transmit message to its subscriber as stream doesn't exist" in {
    val (zkServer, zkClient) = Utils.startZookeeperServer
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
    val streamKey = zookeeperStreamRepository.put(streamBody)
    val streamRecord = zookeeperStreamRepository.get(streamKey).get
    val partition = 1

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
    val status = TransactionStates.Opened
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
      subscriber.receive(timeout = 3000)
    }

    subscriber.close()
    observer.shutdown()
    subscriberNotifier.close()
    zkClient.close()
    zkServer.close()
  }

  it should "transmit message to its subscribers and they will get the same message" in {
    val (zkServer, zkClient) = Utils.startZookeeperServer
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
    val streamKey = zookeeperStreamRepository.put(streamBody)
    val streamRecord = zookeeperStreamRepository.get(streamKey).get
    val partition = 1

    val subscribersNum = 10
    val subscribers = Array.fill(subscribersNum) {
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
    val status = TransactionStates.Opened
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
      Protocol.decode(subscriber.receive(0), TransactionState)
    )

    subscribers.foreach(subscriber => subscriber.close())
    observer.shutdown()
    subscriberNotifier.close()
    zkClient.close()
    zkServer.close()

    data.distinct.length shouldBe 1
  }

}
