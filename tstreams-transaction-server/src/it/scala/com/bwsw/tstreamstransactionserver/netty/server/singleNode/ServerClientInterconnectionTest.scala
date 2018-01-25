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

import java.net.Socket
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicLong, LongAdder}

import com.bwsw.tstreamstransactionserver.configProperties.ClientExecutionContextGrid
import com.bwsw.tstreamstransactionserver.exception.Throwable.ServerConnectionException
import com.bwsw.tstreamstransactionserver.netty.client.zk.ZKClient
import com.bwsw.tstreamstransactionserver.netty.client.{ClientBuilder, InetClient}
import com.bwsw.tstreamstransactionserver.netty.{Protocol, ResponseMessage}
import com.bwsw.tstreamstransactionserver.options._
import com.bwsw.tstreamstransactionserver.rpc._
import com.bwsw.tstreamstransactionserver.util.Implicit.ProducerTransactionSortable
import com.bwsw.tstreamstransactionserver.util.Utils.{getRandomConsumerTransaction, getRandomProducerTransaction, getRandomStream, startZkServerAndGetIt}
import com.bwsw.tstreamstransactionserver.util.{Time, Utils}
import io.netty.buffer.ByteBuf
import io.netty.channel.EventLoopGroup
import io.netty.channel.epoll.{Epoll, EpollEventLoopGroup}
import io.netty.channel.nio.NioEventLoopGroup
import org.apache.curator.retry.RetryForever
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.util.Random

class ServerClientInterconnectionTest
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll {

  private val maxIdleTimeBetweenRecordsMs = 1000
  private val clientsNum = 2

  private lazy val serverBuilder = new SingleNodeServerBuilder()
    .withCommitLogOptions(SingleNodeServerOptions.CommitLogOptions(
      closeDelayMs = Int.MaxValue
    ))

  private lazy val clientBuilder = new ClientBuilder()

  private object TestTimer extends Time {
    private val initialTime = System.currentTimeMillis()
    private var currentTime = initialTime

    override def getCurrentTime: Long = currentTime

    def resetTimer(): Unit = currentTime = initialTime

    def updateTime(newTime: Long): Unit = currentTime = newTime
  }

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

  private def chooseStreamRandomly(streams: IndexedSeq[com.bwsw.tstreamstransactionserver.rpc.StreamValue]) = streams(Random.nextInt(streams.length))

  val secondsWait = 10


  "Client" should "not send requests to the server if it is shutdown" in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { _ =>
      val client = bundle.client

      client.shutdown()
      intercept[IllegalStateException] {
        client.delStream("test_stream")
      }
    }
  }

  it should "retrieve prefix of checkpoint group server" in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { _ =>

      val isShutdown = false

      val clientOpts =
        ClientOptions.ConnectionOptions(
          prefix = bundle.serverBuilder.getCommonRoleOptions.commonMasterPrefix
        )

      val zookeeperOptions =
        bundle.serverBuilder.getZookeeperOptions

      val authOpts =
        ClientOptions.AuthOptions(
          key = bundle.serverBuilder.getAuthenticationOptions.key
        )

      val executionContext =
        new ClientExecutionContextGrid(clientOpts.threadPool)

      val context =
        executionContext.context

      val workerGroup: EventLoopGroup =
        if (Epoll.isAvailable) {
          new EpollEventLoopGroup()
        }
        else {
          new NioEventLoopGroup()
        }

      val zkConnection =
        new ZKClient(
          zookeeperOptions.endpoints,
          zookeeperOptions.sessionTimeoutMs,
          zookeeperOptions.connectionTimeoutMs,
          new RetryForever(zookeeperOptions.retryDelayMs),
          clientOpts.prefix
        ).client

      val requestIdToResponseCommonMap =
        new ConcurrentHashMap[Long, Promise[ByteBuf]](
          20000,
          1.0f,
          clientOpts.threadPool
        )

      val requestIDGen = new AtomicLong(1L)

      val commonInetClient =
        new InetClient(
          zookeeperOptions,
          clientOpts,
          authOpts, {}, {},
          _ => {},
          workerGroup,
          isShutdown,
          zkConnection,
          requestIDGen,
          requestIdToResponseCommonMap,
          context
        )


      val result = commonInetClient.getZKCheckpointGroupServerPrefix()


      Await.ready(result, 10.seconds)

      commonInetClient.shutdown()
    }
  }

  it should "put producer and consumer transactions" in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { _ =>
      val client = bundle.client

      val stream = getRandomStream
      val streamID = Await.result(client.putStream(stream), secondsWait.seconds)

      val producerTransactions = Array.fill(100)(getRandomProducerTransaction(streamID, stream))
      val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(streamID, stream))

      val result = client.putTransactions(producerTransactions, consumerTransactions)

      Await.result(result, 10.seconds) shouldBe true
    }
  }


  it should "delete stream, that doesn't exist in database on the server and get result" in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { _ =>
      val client = bundle.client

      Await.result(client.delStream("test_stream"), secondsWait.seconds) shouldBe false
    }
  }

  it should "put stream, then delete that stream and check it doesn't exist" in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { _ =>
      val client = bundle.client

      val stream = getRandomStream

      val streamID = Await.result(client.putStream(stream), secondsWait.seconds)
      streamID shouldBe 0
      Await.result(client.checkStreamExists(stream.name), secondsWait.seconds) shouldBe true
      Await.result(client.delStream(stream.name), secondsWait.seconds) shouldBe true
      Await.result(client.checkStreamExists(stream.name), secondsWait.seconds) shouldBe false
    }
  }

  it should "put stream, then delete that stream, then again delete this stream and get that operation isn't successful" in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { _ =>
      val client = bundle.client
      val stream = getRandomStream

      val streamID = Await.result(client.putStream(stream), secondsWait.seconds)
      streamID shouldBe 0
      Await.result(client.delStream(stream.name), secondsWait.seconds) shouldBe true
      Await.result(client.checkStreamExists(stream.name), secondsWait.seconds) shouldBe false
      Await.result(client.delStream(stream.name), secondsWait.seconds) shouldBe false
    }
  }

  it should "put stream, then delete this stream, and server should save producer and consumer transactions on putting them by client" in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { transactionServer =>
      val client = bundle.client

      val stream = getRandomStream
      val streamID = Await.result(client.putStream(stream), secondsWait.seconds)
      val producerTransactions = Array.fill(100)(getRandomProducerTransaction(streamID, stream)).filter(_.state == TransactionStates.Opened)
      val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(streamID, stream))

      Await.result(client.putTransactions(producerTransactions, consumerTransactions), secondsWait.seconds)

      //it's required to a CommitLogToBerkeleyWriter writes the producer transactions to db
      transactionServer.scheduledCommitLog.run()
      transactionServer.commitLogToRocksWriter.run()

      val fromID = producerTransactions.minBy(_.transactionID).transactionID
      val toID = producerTransactions.maxBy(_.transactionID).transactionID

      val resultBeforeDeleting = Await.result(client.scanTransactions(streamID, stream.partitions, fromID, toID, Int.MaxValue, Set()), secondsWait.seconds).producerTransactions
      resultBeforeDeleting should not be empty

      Await.result(client.delStream(stream.name), secondsWait.seconds)
      Await.result(client.scanTransactions(streamID, stream.partitions, fromID, toID, Int.MaxValue, Set()), secondsWait.seconds)
        .producerTransactions should contain theSameElementsInOrderAs resultBeforeDeleting
    }
  }

  it should "throw an exception when the server isn't available" in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { transactionServer =>
      val client = bundle.client

      val stream = getRandomStream
      val streamID = Await.result(client.putStream(stream), secondsWait.seconds)

      val producerTransactions = Array.fill(100000)(getRandomProducerTransaction(streamID, stream))
      val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(streamID, stream))

      val resultInFuture = client.putTransactions(producerTransactions, consumerTransactions)

      transactionServer.shutdown()

      a[ServerConnectionException] shouldBe thrownBy {
        Await.result(resultInFuture, secondsWait.seconds)
      }
    }
  }

  it should "throw an exception when the server restarted" in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    val client = bundle.client
    val transactionServer = bundle.transactionServer

    val stream = getRandomStream
    val producerTransactions = Array.fill(10000)(getRandomProducerTransaction(1, stream))
    val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(1, stream))


    val resultInFuture = client.putTransactions(producerTransactions, consumerTransactions)


    transactionServer.shutdown()
    val secondServer = bundle.serverBuilder
      .withBootstrapOptions(SingleNodeServerOptions.BootstrapOptions(bindPort = Utils.getRandomPort))
      .build()

    val task = new Thread(
      () => secondServer.start()
    )

    task.start()

    /*
     * The type of this exception can be either MasterLostException or ServerUnreachableException,
     * depends what happens earlier: updation of master node in ZooKeeper or
     * invocation InetClient.onServerConnectionLostDefaultBehaviour() from NettyConnectionHandler
     */
    a[ServerConnectionException] shouldBe thrownBy {
      Await.result(resultInFuture, 10000.seconds)
    }

    secondServer.shutdown()
    task.interrupt()
    bundle.closeDbsAndDeleteDirectories()
  }

  it should "put any kind of binary data and get it back" in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { _ =>
      val client = bundle.client

      val stream = getRandomStream
      val streamID = Await.result(client.putStream(stream), secondsWait.seconds)

      val txn = getRandomProducerTransaction(streamID, stream)
      Await.result(client.putProducerState(txn), secondsWait.seconds)

      val dataAmount = 5000
      val data = Array.fill(dataAmount)(Random.nextString(10).getBytes)

      val resultInFuture = Await.result(client.putTransactionData(streamID, txn.partition, txn.transactionID, data, 0), secondsWait.seconds)
      resultInFuture shouldBe true

      val dataFromDatabase = Await.result(client.getTransactionData(streamID, txn.partition, txn.transactionID, 0, dataAmount), secondsWait.seconds)
      data should contain theSameElementsAs dataFromDatabase
    }
  }

  it should "[putProducerStateWithData] put a producer transaction (Opened) with data, and server should persist data." in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { transactionServer =>
      val client = bundle.client

      //arrange
      val stream =
        getRandomStream

      val streamID =
        Await.result(client.putStream(stream), secondsWait.seconds)

      val openedProducerTransaction =
        getRandomProducerTransaction(streamID, stream, TransactionStates.Opened)

      val dataAmount = 30
      val data = Array.fill(dataAmount)(Random.nextString(10).getBytes)

      val from = dataAmount
      val to = 2 * from
      Await.result(client.putProducerStateWithData(openedProducerTransaction, data, from), secondsWait.seconds)

      //it's required to a CommitLogToBerkeleyWriter writes the producer transactions to db
      transactionServer.scheduledCommitLog.run()
      transactionServer.commitLogToRocksWriter.run()

      val successResponse = Await.result(
        client.getTransaction(streamID, stream.partitions, openedProducerTransaction.transactionID
        ), secondsWait.seconds)

      val successResponseData = Await.result(
        client.getTransactionData(
          streamID, stream.partitions, openedProducerTransaction.transactionID, from, to
        ), secondsWait.seconds)


      //assert
      successResponse shouldBe TransactionInfo(
        exists = true,
        Some(openedProducerTransaction)
      )

      successResponseData should contain theSameElementsInOrderAs data
    }
  }

  it should "[scanTransactions] put transactions and get them back" in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { transactionServer =>
      val client = bundle.client
      val stream = getRandomStream
      val streamID = Await.result(client.putStream(stream), secondsWait.seconds)

      val producerTransactions = Array.fill(30)(getRandomProducerTransaction(streamID, stream)).filter(_.state == TransactionStates.Opened) :+
        getRandomProducerTransaction(streamID, stream).copy(state = TransactionStates.Opened)

      val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(streamID, stream))

      Await.result(client.putTransactions(producerTransactions, consumerTransactions), secondsWait.seconds)

      val statesAllowed = Array(TransactionStates.Opened, TransactionStates.Updated)
      val (from, to) = (
        producerTransactions.filter(txn => statesAllowed.contains(txn.state)).minBy(_.transactionID).transactionID,
        producerTransactions.filter(txn => statesAllowed.contains(txn.state)).maxBy(_.transactionID).transactionID
      )


      Await.result(client.putConsumerCheckpoint(getRandomConsumerTransaction(streamID, stream)), secondsWait.seconds)
      transactionServer.scheduledCommitLog.run()
      transactionServer.commitLogToRocksWriter.run()


      val resFrom_1From = Await.result(client.scanTransactions(streamID, stream.partitions, from - 1, from, Int.MaxValue, Set()), secondsWait.seconds)
      resFrom_1From.producerTransactions.size shouldBe 1
      resFrom_1From.producerTransactions.head.transactionID shouldBe from


      val resFromFrom = Await.result(client.scanTransactions(streamID, stream.partitions, from, from, Int.MaxValue, Set()), secondsWait.seconds)
      resFromFrom.producerTransactions.size shouldBe 1
      resFromFrom.producerTransactions.head.transactionID shouldBe from


      val resToFrom = Await.result(client.scanTransactions(streamID, stream.partitions, to, from, Int.MaxValue, Set()), secondsWait.seconds)
      resToFrom.producerTransactions.size shouldBe 0

      val producerTransactionsByState = producerTransactions.groupBy(_.state)
      val res = Await.result(client.scanTransactions(streamID, stream.partitions, from, to, Int.MaxValue, Set()), secondsWait.seconds).producerTransactions

      val producerOpenedTransactions = producerTransactionsByState(TransactionStates.Opened).sortBy(_.transactionID)

      res.head shouldBe producerOpenedTransactions.head
      res shouldBe sorted
    }
  }

  it should "disconnect from the server when it is off" in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { server =>
      val client = bundle.client

      Thread.sleep(1000) // wait until client connected to server
      client.isConnected shouldBe true

      server.shutdown()
      Thread.sleep(1000) // wait until client disconnected from server

      client.isConnected shouldBe false
    }
  }


  "getTransaction" should "not get a producer transaction if there's no transaction" in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { _ =>
      val client = bundle.client

      //arrange
      val stream = getRandomStream
      val fakeTransactionID = System.nanoTime()

      //act
      val streamID = Await.result(client.putStream(stream), secondsWait.seconds)
      val response = Await.result(client.getTransaction(streamID, stream.partitions, fakeTransactionID), secondsWait.seconds)

      //assert
      response shouldBe TransactionInfo(exists = false, None)
    }
  }


  it should "put a producer transaction (Opened), return it and shouldn't return a producer transaction which id is greater (getTransaction)" in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { transactionServer =>
      val client = bundle.client
      //arrange
      val stream = getRandomStream
      val streamID = Await.result(client.putStream(stream), secondsWait.seconds)
      val openedProducerTransaction = getRandomProducerTransaction(streamID, stream, TransactionStates.Opened)
      val fakeTransactionID = openedProducerTransaction.transactionID + 1

      //act
      Await.result(client.putProducerState(openedProducerTransaction), secondsWait.seconds)

      //it's required to a CommitLogToBerkeleyWriter writes the producer transactions to db
      transactionServer.scheduledCommitLog.run()
      transactionServer.commitLogToRocksWriter.run()

      val successResponse = Await.result(client.getTransaction(streamID, stream.partitions, openedProducerTransaction.transactionID), secondsWait.seconds)
      val failedResponse = Await.result(client.getTransaction(streamID, stream.partitions, fakeTransactionID), secondsWait.seconds)

      //assert
      successResponse shouldBe TransactionInfo(exists = true, Some(openedProducerTransaction))
      failedResponse shouldBe TransactionInfo(exists = false, None)
    }
  }

  it should "put a producer transaction (Opened), return it and shouldn't return a non-existent producer transaction (getTransaction)" in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { transactionServer =>
      val client = bundle.client
      //arrange
      val stream = getRandomStream
      val streamID =
        Await.result(client.putStream(stream), secondsWait.seconds)
      val openedProducerTransaction =
        getRandomProducerTransaction(streamID, stream, TransactionStates.Opened)
      val fakeTransactionID =
        openedProducerTransaction.transactionID - 1

      //act)
      Await.result(client.putProducerState(openedProducerTransaction), secondsWait.seconds)

      //it's required to a CommitLogToBerkeleyWriter writes the producer transactions to db
      transactionServer.scheduledCommitLog.run()
      transactionServer.commitLogToRocksWriter.run()

      val successResponse =
        Await.result(
          client.getTransaction(
            streamID,
            stream.partitions,
            openedProducerTransaction.transactionID
          ),
          secondsWait.seconds
        )
      val failedResponse =
        Await.result(
          client.getTransaction(
            streamID,
            stream.partitions,
            fakeTransactionID
          ),
          secondsWait.seconds
        )

      //assert
      successResponse shouldBe TransactionInfo(exists = true, Some(openedProducerTransaction))
      failedResponse shouldBe TransactionInfo(exists = true, None)
    }
  }

  it should "put a producer transaction (Opened) and get it back (getTransaction)" in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { transactionServer =>
      val client = bundle.client
      //arrange
      val stream = getRandomStream
      val streamID = Await.result(client.putStream(stream), secondsWait.seconds)
      val openedProducerTransaction = getRandomProducerTransaction(streamID, stream, TransactionStates.Opened)

      //act
      Await.result(client.putProducerState(openedProducerTransaction), secondsWait.seconds)

      //it's required to a CommitLogToBerkeleyWriter writes the producer transactions to db
      transactionServer.scheduledCommitLog.run()
      transactionServer.commitLogToRocksWriter.run()

      val response = Await.result(client.getTransaction(streamID, stream.partitions, openedProducerTransaction.transactionID), secondsWait.seconds)

      //assert
      response shouldBe TransactionInfo(exists = true, Some(openedProducerTransaction))
    }
  }

  it should "put consumerCheckpoint and get a transaction id back" in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { transactionServer =>
      val client = bundle.client
      val stream = getRandomStream
      val streamID = Await.result(client.putStream(stream), secondsWait.seconds)

      val consumerTransaction = getRandomConsumerTransaction(streamID, stream)

      Await.result(client.putConsumerCheckpoint(consumerTransaction), secondsWait.seconds)
      transactionServer.scheduledCommitLog.run()
      transactionServer.commitLogToRocksWriter.run()

      val consumerState = Await.result(client.getConsumerState(consumerTransaction.name, streamID, consumerTransaction.partition), secondsWait.seconds)

      consumerState shouldBe consumerTransaction.transactionID
    }
  }

  "Server" should "not have any problems with many clients" in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder, clientsNum
    )

    bundle.operate { _ =>
      val clients = bundle.clients
      val client = bundle.clients.head

      val streams = Array.fill(10000)(getRandomStream)
      Await.result(client.putStream(chooseStreamRandomly(streams)), secondsWait.seconds)

      val dataCounter = new java.util.concurrent.ConcurrentHashMap[(Int, Int), LongAdder]()

      def addDataLength(streamID: Int, partition: Int, dataLength: Int): Unit = {
        val valueToAdd = if (dataCounter.containsKey((streamID, partition))) dataLength else 0
        dataCounter.computeIfAbsent((streamID, partition), _ => new LongAdder()).add(valueToAdd)
      }

      def getDataLength(streamID: Int, partition: Int) = dataCounter.get((streamID, partition)).intValue()


      val res: Future[mutable.ArraySeq[Boolean]] = Future.sequence(clients map { client =>
        val streamFake = getRandomStream
        client.putStream(streamFake).flatMap { streamID =>
          val producerTransactions = Array.fill(100)(getRandomProducerTransaction(streamID, streamFake))
          val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(streamID, streamFake))
          val data = Array.fill(100)(Random.nextInt(10000).toString.getBytes)

          client.putTransactions(producerTransactions, consumerTransactions)

          val (stream, partition) = (producerTransactions.head.stream, producerTransactions.head.partition)
          addDataLength(streamID, partition, data.length)
          val txn = producerTransactions.head
          client.putTransactionData(streamID, txn.partition, txn.transactionID, data, getDataLength(stream, partition))
        }
      })

      all(Await.result(res, (secondsWait * clientsNum).seconds)) shouldBe true
      clients.foreach(_.shutdown())
    }
  }

  "Server" should "return only transactions up to 1st incomplete(transaction after Opened one)" in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { transactionServer =>
      val client = bundle.client

      val stream = getRandomStream
      val streamID = Await.result(client.putStream(stream), secondsWait.seconds)

      val FIRST = 30
      val LAST = 100
      val partition = 1

      val transactions1 = for (_ <- 0 until FIRST) yield {
        TestTimer.updateTime(TestTimer.getCurrentTime + 1L)
        TestTimer.getCurrentTime
      }

      Await.result(client.putTransactions(transactions1.flatMap { t =>
        Array(
          ProducerTransaction(streamID, partition, t, TransactionStates.Opened, 1, 120L),
          ProducerTransaction(streamID, partition, t, TransactionStates.Checkpointed, 1, 120L)
        )
      }, Seq()), secondsWait.seconds)

      TestTimer.updateTime(TestTimer.getCurrentTime + maxIdleTimeBetweenRecordsMs)
      Await.result(client.putConsumerCheckpoint(getRandomConsumerTransaction(streamID, stream)), secondsWait.seconds)
      transactionServer.scheduledCommitLog.run()
      transactionServer.commitLogToRocksWriter.run()

      TestTimer.updateTime(TestTimer.getCurrentTime + 1L)
      Await.result(client.putProducerState(ProducerTransaction(streamID, partition, TestTimer.getCurrentTime, TransactionStates.Opened, 1, 120L)), secondsWait.seconds)

      TestTimer.updateTime(TestTimer.getCurrentTime + maxIdleTimeBetweenRecordsMs)
      Await.result(client.putConsumerCheckpoint(getRandomConsumerTransaction(streamID, stream)), secondsWait.seconds)
      transactionServer.scheduledCommitLog.run()
      transactionServer.commitLogToRocksWriter.run()

      val transactions2 = for (_ <- FIRST until LAST) yield {
        TestTimer.updateTime(TestTimer.getCurrentTime + 1L)
        TestTimer.getCurrentTime
      }

      Await.result(client.putTransactions(transactions2.flatMap { t =>
        Array(
          ProducerTransaction(streamID, partition, t, TransactionStates.Opened, 1, 120L),
          ProducerTransaction(streamID, partition, t, TransactionStates.Checkpointed, 1, 120L)
        )
      }, Seq()), secondsWait.seconds)

      TestTimer.updateTime(TestTimer.getCurrentTime + maxIdleTimeBetweenRecordsMs)
      Await.result(client.putConsumerCheckpoint(getRandomConsumerTransaction(streamID, stream)), secondsWait.seconds)
      transactionServer.commitLogToRocksWriter.run()

      val transactions = transactions1 ++ transactions2
      val firstTransaction = transactions.head
      val lastTransaction = transactions.last

      val res = Await.result(client.scanTransactions(streamID, partition, firstTransaction, lastTransaction, Int.MaxValue, Set(TransactionStates.Opened)), secondsWait.seconds)

      res.producerTransactions.size shouldBe transactions1.size
    }
  }

  it should "update client's token TTL until client is disconnect" in {
    val ttlSec = 2
    val ttlMs = ttlSec * 1000
    val keepAliveThreshold = 3
    val keepAliveIntervalMs = ttlMs / keepAliveThreshold
    val authenticationOptions = serverBuilder.getAuthenticationOptions.copy(
      tokenTtlSec = ttlSec)
    val connectionOptions = clientBuilder.getConnectionOptions.copy(
      keepAliveIntervalMs = keepAliveIntervalMs,
      keepAliveThreshold = keepAliveThreshold)

    // TODO: find other way to retrieve client token
    val seed = 0
    Random.setSeed(seed)
    Random.nextInt()
    val token = Random.nextInt()
    Random.setSeed(seed)

    val bundle = Utils.startTransactionServerAndClient(
      zkClient,
      serverBuilder.withAuthenticationOptions(authenticationOptions),
      clientBuilder.withConnectionOptions(connectionOptions))

    bundle.operate { _ =>
      val bootstrapOptions = bundle.serverBuilder.getBootstrapOptions
      tokenIsValid(token, bootstrapOptions.bindHost, bootstrapOptions.bindPort) shouldBe true
      Thread.sleep(ttlMs)
      tokenIsValid(token, bootstrapOptions.bindHost, bootstrapOptions.bindPort) shouldBe true
      bundle.client.shutdown()
      tokenIsValid(token, bootstrapOptions.bindHost, bootstrapOptions.bindPort) shouldBe true
      Thread.sleep(ttlMs)
      tokenIsValid(token, bootstrapOptions.bindHost, bootstrapOptions.bindPort) shouldBe false
    }
  }


  private def tokenIsValid(token: Int, host: String, port: Int): Boolean = {
    val request = Protocol.IsValid.encodeRequestToMessage(
      TransactionService.IsValid.Args(token))(
      1L,
      token,
      isFireAndForgetMethod = false)

    val bytes = request.toByteArray
    val socket = new Socket(host, port)
    val inputStream = socket.getInputStream
    val outputStream = socket.getOutputStream
    outputStream.write(bytes)
    outputStream.flush()

    def loop(lost: Int): Unit = {
      if (lost > 0 && inputStream.available() == 0) {
        Thread.sleep(100)
        loop(lost - 1)
      }
    }

    loop(10)

    val responseBytes = new Array[Byte](inputStream.available())
    inputStream.read(responseBytes)
    socket.close()

    val response = ResponseMessage.fromByteArray(responseBytes)
    val result = Protocol.IsValid.decodeResponse(response)

    result.success shouldBe defined

    result.success.get
  }
}
