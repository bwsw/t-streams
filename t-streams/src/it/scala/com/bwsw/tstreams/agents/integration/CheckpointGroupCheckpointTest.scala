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

package com.bwsw.tstreams.agents.integration

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.consumer.Consumer
import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.consumer.subscriber.Callback
import com.bwsw.tstreams.agents.producer.{NewProducerTransactionPolicy, Producer}
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.SingleNodeTestingServer
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates.{Checkpointed, Invalid, Opened}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers}

import scala.util.Try

class CheckpointGroupCheckpointTest
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with TestUtils {

  private val partitionsCount = 10
  private val partitions: Set[Int] = (0 until partitionsCount).toSet
  private val waitingTimeout = 5000

  private var server: SingleNodeTestingServer = _

  private def createProducer(): Producer = f.getProducer(
    name = s"test_producer-$id",
    partitions = partitions)

  private def createConsumer(): Consumer = f.getConsumer(
    name = s"test_consumer-$id",
    partitions = partitions,
    offset = Oldest,
    useLastOffset = true)

  private def createSubscriber(callback: Callback) = f.getSubscriber(
    name = s"test_subscriber-$id",
    partitions = partitions,
    callback = callback,
    offset = Oldest)

  override protected def beforeEach(): Unit = {
    server = TestStorageServer.getNewClean()
    createNewStream(partitions = partitionsCount)
  }

  override protected def afterEach(): Unit = {
    TestStorageServer.dispose(server)
  }


  "Group commit" should "checkpoint all AgentsGroup state" in {
    val producer = createProducer()
    val consumer = createConsumer()
    val consumer2 = createConsumer()
    consumer.start()

    val group = f.getCheckpointGroup()
    group.add(producer)
    group.add(consumer)

    val l1 = new CountDownLatch(1)
    val l2 = new CountDownLatch(1)

    val transaction1 = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)

    server.notifyProducerTransactionCompleted(t =>
      transaction1.getTransactionID == t.transactionID && t.state == Checkpointed, l1.countDown())

    logger.info("Transaction 1 is " + transaction1.getTransactionID.toString)
    transaction1.send("info1".getBytes())
    transaction1.checkpoint()

    l1.await()

    //move consumer offsets
    consumer.getTransaction(0).get

    //open transaction without close
    val transaction2 = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened, 1)

    server.notifyProducerTransactionCompleted(t =>
      transaction2.getTransactionID == t.transactionID && t.state == Checkpointed, l2.countDown())

    logger.info("Transaction 2 is " + transaction2.getTransactionID.toString)
    transaction2.send("info2".getBytes())

    group.checkpoint()

    l2.await()

    consumer2.start()
    //assert that the second transaction was closed and consumer offsets was moved
    consumer2.getTransaction(1).get.getAll.head shouldBe "info2".getBytes()
  }


  it should "checkpoint transactions properly" in {
    val partition = partitions.head

    val producer = createProducer()
    val producer2 = createProducer()
    val consumer = createConsumer()
    consumer.start()

    val group = f.getCheckpointGroup()
    group.add(producer)
    group.add(producer2)

    val nonEmptyTransaction1 = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened, partition)
    nonEmptyTransaction1.send("data")
    group.checkpoint()

    val emptyTransaction1 = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened, partition)
    val nonEmptyTransaction2 = producer2.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened, partition)
    nonEmptyTransaction2.send("data")
    group.checkpoint()

    val nonEmptyTransaction3 = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened, partition)
    nonEmptyTransaction3.send("data")
    val emptyTransaction2 = producer2.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened, partition)
    group.checkpoint()

    val nonEmptyTransaction4 = producer2.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened, partition)
    val latch = new CountDownLatch(1)
    server.notifyProducerTransactionCompleted(
      t => t.transactionID == nonEmptyTransaction4.getTransactionID && t.state == Checkpointed,
      latch.countDown())

    nonEmptyTransaction4.send("data")
    group.checkpoint()

    latch.await(waitingTimeout, TimeUnit.MILLISECONDS)

    checkTransactions(
      consumer,
      partition,
      Table(
        ("transaction", "state"),
        (nonEmptyTransaction1, Checkpointed),
        (nonEmptyTransaction2, Checkpointed),
        (nonEmptyTransaction3, Checkpointed),
        (nonEmptyTransaction4, Checkpointed),
        (emptyTransaction1, Invalid),
        (emptyTransaction2, Invalid)))

    group.stop()
    producer.close()
    producer2.close()
    consumer.close()
  }


  it should "cancel transactions properly" in {
    val partition = partitions.head

    val producer = createProducer()
    val producer2 = createProducer()
    val consumer = createConsumer()
    consumer.start()

    val group = f.getCheckpointGroup()
    group.add(producer)
    group.add(producer2)

    val nonEmptyTransaction1 = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened, partition)
    nonEmptyTransaction1.send("data")
    group.cancel()

    val emptyTransaction1 = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened, partition)
    val nonEmptyTransaction2 = producer2.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened, partition)
    nonEmptyTransaction2.send("data")
    group.cancel()

    val nonEmptyTransaction3 = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened, partition)
    nonEmptyTransaction3.send("data")
    val emptyTransaction2 = producer2.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened, partition)
    group.cancel()

    val nonEmptyTransaction4 = producer2.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened, partition)
    val latch = new CountDownLatch(1)
    server.notifyProducerTransactionCompleted(
      t => t.transactionID == nonEmptyTransaction4.getTransactionID && t.state == Invalid,
      latch.countDown())

    nonEmptyTransaction4.send("data")
    group.cancel()

    latch.await(waitingTimeout, TimeUnit.MILLISECONDS)

    checkTransactions(
      consumer,
      partition,
      Table(
        ("transaction", "state"),
        (nonEmptyTransaction1, Invalid),
        (nonEmptyTransaction2, Invalid),
        (nonEmptyTransaction3, Invalid),
        (nonEmptyTransaction4, Invalid),
        (emptyTransaction1, Invalid),
        (emptyTransaction2, Invalid)))

    group.stop()
    producer.close()
    producer2.close()
    consumer.close()
  }


  it should "throw an exception instead of checkpoint when one of the agents isn't connected or all of them are not " +
    "connected to a server" in {
    val agentsCount = 5
    val storageClient = f.getStorageClient()
    val checkingConsumer = createConsumer().start()

    def test(disconnectedAgentId: Option[Int]) = {
      val partition = 0
      val producer1 = createProducer()
      val producer2 = createProducer()
      val consumer1 = createConsumer().start()
      val consumer2 = createConsumer().start()
      val subscriber = createSubscriber((_, _) => {}).start()

      val stream = producer1.stream.id
      val consumerNames = Seq(consumer1, consumer2, subscriber).map(_.getAgentName())
      val producers = Seq(producer1, producer2)
      producers.foreach(_.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened, partition).send("data"))
      producers.foreach(_.checkpoint())

      Seq(consumer1, consumer2).foreach(_.getTransaction(partition))

      val checkpointGroup = f.getCheckpointGroup()
      val agents = IndexedSeq(
        producer1,
        producer2,
        consumer1,
        consumer2,
        subscriber)

      agents.length shouldBe agentsCount
      agents.foreach(checkpointGroup.add)

      val transaction1 = producer1.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened, partition)
      transaction1.send("data")

      val transaction2 = producer2.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened, partition)
      transaction2.send("data")

      disconnectedAgentId match {
        case Some(j) => agents(j).getStorageClient().shutdown()
        case None => agents.foreach(_.getStorageClient().shutdown())
      }

      an[IllegalStateException] shouldBe thrownBy {
        checkpointGroup.checkpoint()
      }

      checkTransactions(
        checkingConsumer,
        partition,
        Table(
          ("transaction", "state"),
          (transaction1, Opened),
          (transaction2, Opened)))

      consumerNames.foreach { name =>
        storageClient.getLastSavedConsumerOffset(name, stream, partition) shouldBe -1
      }

      agents.foreach(agent => Try(agent.close()))
      checkpointGroup.stop()
    }

    forAll(Table("agentId", 0 until agentsCount: _*))(agentId => test(Some(agentId)))
    test(None)

    checkingConsumer.close()
    storageClient.shutdown()
  }


  override protected def afterAll(): Unit = onAfterAll()
}
