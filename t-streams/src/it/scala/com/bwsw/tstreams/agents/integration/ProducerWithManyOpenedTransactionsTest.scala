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
import com.bwsw.tstreams.agents.producer._
import com.bwsw.tstreams.testutils._
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}


class ProducerWithManyOpenedTransactionsTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  private lazy val server = TestStorageServer.getNewClean()

  private lazy val producer = f.getProducer(
    name = "test_producer",
    partitions = Set(0, 1, 2))

  private lazy val consumer = f.getConsumer(
    name = "test_consumer",
    partitions = Set(0, 1, 2),
    offset = Oldest,
    useLastOffset = true)

  override def beforeAll(): Unit = {
    server
    createNewStream()
    consumer.start()
  }


  "BasicProducer.newTransaction()" should "return BasicProducerTransaction instance" in {
    val data1 = (for (_ <- 0 until 10) yield randomKeyspace).sorted
    val data2 = (for (_ <- 0 until 10) yield randomKeyspace).sorted
    val data3 = (for (_ <- 0 until 10) yield randomKeyspace).sorted


    val transaction1: ProducerTransactionImpl = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)
    val transaction2: ProducerTransactionImpl = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)
    val transaction3: ProducerTransactionImpl = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)

    val l = new CountDownLatch(1)
    server.notifyProducerTransactionCompleted(t => t.transactionID == transaction1.getTransactionID && t.state == TransactionStates.Checkpointed, l.countDown())

    data1.foreach(x => transaction1.send(x))
    data2.foreach(x => transaction2.send(x))
    data3.foreach(x => transaction3.send(x))

    transaction3.checkpoint()
    transaction2.checkpoint()
    transaction1.checkpoint()

    l.await()

    consumer.getTransaction(0).get.getAll.map(i => new String(i)).sorted shouldBe data1
    consumer.getTransaction(1).get.getAll.map(i => new String(i)).sorted shouldBe data2
    consumer.getTransaction(2).get.getAll.map(i => new String(i)).sorted shouldBe data3

    (0 to 2).foreach(p => consumer.getTransaction(p).isEmpty shouldBe true)
  }

  "BasicProducer.newTransaction()" should "return error if try to open more than 3 transactions for 3 partitions if ProducerPolicies.errorIfOpened" in {
    producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)
    producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)
    producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)

    val r: Boolean = try {
      producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)
      false
    } catch {
      case _: IllegalStateException => true
    }
    r shouldBe true
    producer.checkpoint()
  }

  "BasicProducer.newTransaction()" should "return ok if try to open more than 3 transactions for 3 partitions if ProducerPolicies.checkpointIfOpened" in {
    producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)
    producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)
    producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)

    val r: Boolean = try {
      producer.newTransaction(NewProducerTransactionPolicy.CheckpointIfOpened)
      true
    } catch {
      case _: IllegalStateException => false
    }
    r shouldBe true
    producer.checkpoint()
  }

  "BasicProducer.newTransaction()" should "return ok if try to open more than 3 transactions for 3 partitions if ProducerPolicies.cancelIfOpened" in {
    producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)
    producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)
    producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)

    val r: Boolean = try {
      producer.newTransaction(NewProducerTransactionPolicy.CancelIfOpened)
      true
    } catch {
      case _: IllegalStateException => false
    }
    r shouldBe true
    producer.checkpoint()
  }

  override def afterAll(): Unit = {
    producer.stop()
    consumer.stop()
    TestStorageServer.dispose(server)
    onAfterAll()
  }
}
