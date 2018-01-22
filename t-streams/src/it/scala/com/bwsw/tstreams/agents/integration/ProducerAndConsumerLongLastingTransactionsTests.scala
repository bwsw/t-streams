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

import java.util.concurrent.CountDownLatch

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.producer.NewProducerTransactionPolicy
import com.bwsw.tstreams.testutils._
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class ProducerAndConsumerLongLastingTransactionsTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  private lazy val server = TestStorageServer.getNewClean()

  private lazy val producer1 = f.getProducer(
    name = "test_producer",
    partitions = Set(0))

  private lazy val producer2 = f.getProducer(
    name = "test_producer",
    partitions = Set(0))

  private lazy val consumer = f.getConsumer(
    name = "test_consumer",
    partitions = Set(0),
    offset = Oldest,
    useLastOffset = true)

  override def beforeAll(): Unit = {
    server
    createNewStream()
    consumer.start()
  }

  "Producers" should "first producer - generate transactions lazily, second producer - generate transactions faster" +
    " than the first one but with pause at the very beginning, consumer - retrieve all transactions which was sent" in {
    val totalElementsInTransaction = 10
    val dataToSend1: List[String] = (for (_ <- 0 until totalElementsInTransaction) yield "data_to_send_pr1_" + randomKeyspace).toList.sorted
    val dataToSend2: List[String] = (for (_ <- 0 until totalElementsInTransaction) yield "data_to_send_pr2_" + randomKeyspace).toList.sorted

    val waitFirstAtConsumer = new CountDownLatch(1)
    val waitSecondAtConsumer = new CountDownLatch(1)
    val waitFirstAtProducer = new CountDownLatch(1)
    val waitSecondAtProducer = new CountDownLatch(1)

    val transaction1 = producer1.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)
    val transaction2 = producer2.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)
    server.notifyProducerTransactionCompleted(t => t.transactionID == transaction1.getTransactionID && t.state == TransactionStates.Checkpointed, waitFirstAtConsumer.countDown())
    server.notifyProducerTransactionCompleted(t => t.transactionID == transaction2.getTransactionID && t.state == TransactionStates.Checkpointed, waitSecondAtConsumer.countDown())

    val producer1Thread = new Thread(() => {
      waitFirstAtProducer.countDown()
      dataToSend1.foreach { x => transaction1.send(x.getBytes()) }
      waitSecondAtProducer.await()
      transaction1.checkpoint()
    })

    val producer2Thread = new Thread(() => {
      waitFirstAtProducer.await()
      dataToSend2.foreach { x => transaction2.send(x.getBytes()) }
      transaction2.checkpoint()
      waitSecondAtProducer.countDown()
    })

    Seq(producer1Thread, producer2Thread).foreach(t => t.start())

    waitFirstAtConsumer.await()
    val transaction1Opt = consumer.getTransaction(0)
    transaction1Opt.get.getTransactionID shouldBe transaction1.getTransactionID
    val data1 = transaction1Opt.get.getAll.map(i => new String(i)).toList.sorted
    data1 shouldBe dataToSend1

    waitSecondAtConsumer.await()
    val transaction2Opt = consumer.getTransaction(0)
    transaction2Opt.get.getTransactionID shouldBe transaction2.getTransactionID
    val data2 = transaction2Opt.get.getAll.map(i => new String(i)).toList.sorted
    data2 shouldBe dataToSend2

    producer1Thread.join()
    producer2Thread.join()
  }

  override def afterAll(): Unit = {
    producer1.stop()
    producer2.stop()
    consumer.stop()
    TestStorageServer.dispose(server)
    onAfterAll()
  }
}