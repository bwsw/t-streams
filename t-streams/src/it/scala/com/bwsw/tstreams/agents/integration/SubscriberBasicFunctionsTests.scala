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

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.agents.producer.NewProducerTransactionPolicy
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable

/**
  * Created by Ivan Kudryavtsev on 24.08.16.
  */
class SubscriberBasicFunctionsTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  private lazy val server = TestStorageServer.getNewClean()

  private lazy val producer = f.getProducer(
    name = "test_producer",
    partitions = Set(0, 1, 2))

  override def beforeAll(): Unit = {
    server
    createNewStream()
  }

  "SUbsctiber" should "start and stop with default options" in {
    val s = f.getSubscriber(name = "sv2",
      partitions = Set(0, 1, 2),
      offset = Oldest,
      useLastOffset = true,
      callback = (_: TransactionOperator, _: ConsumerTransaction) => {})
    s.start()
    s.stop()
  }

  it should "not allow double start" in {
    val s = f.getSubscriber(name = "sv2",
      partitions = Set(0, 1, 2),
      offset = Oldest,
      useLastOffset = true,
      callback = (_: TransactionOperator, _: ConsumerTransaction) => {})
    s.start()
    var flag = false
    flag = try {
      s.start()
      false
    } catch {
      case _: IllegalStateException =>
        true
    }
    flag shouldBe true
    s.stop()
  }

  it should "not allow double stop" in {
    val s = f.getSubscriber(name = "sv2",
      partitions = Set(0, 1, 2),
      offset = Oldest,
      useLastOffset = true,
      callback = (_: TransactionOperator, _: ConsumerTransaction) => {})
    s.start()
    s.stop()
    var flag = false
    flag = try {
      s.stop()
      false
    } catch {
      case _: IllegalStateException =>
        true
    }
    flag shouldBe true
  }

  it should "allow to be created with in memory queues" in {
    val f1 = f.copy()
    val s = f1.getSubscriber(name = "sv2_inram",
      partitions = Set(0, 1, 2),
      offset = Oldest,
      useLastOffset = true,
      callback = (_: TransactionOperator, _: ConsumerTransaction) => {})
    s.start()
    s.stop()
  }

  it should "receive all transactions producer by producer previously" in {
    val l = new CountDownLatch(1)
    val pl = new CountDownLatch(1)
    var i: Int = 0
    val TOTAL = 1000
    var id: Long = 0
    val ps = mutable.ListBuffer[Long]()
    for (_ <- 0 until TOTAL) {
      val transaction = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)
      transaction.send("test")
      transaction.checkpoint()
      id = transaction.getTransactionID
      ps.append(id)
    }
    server.notifyProducerTransactionCompleted(t => t.transactionID == id && t.state == TransactionStates.Checkpointed, pl.countDown())
    producer.stop()
    pl.await()

    val ss = mutable.ListBuffer[Long]()

    val s = f.getSubscriber(name = "sv2",
      partitions = Set(0, 1, 2),
      offset = Oldest,
      useLastOffset = true,
      callback = (_: TransactionOperator, transaction: ConsumerTransaction) => this.synchronized {
        ss.append(transaction.getTransactionID)
        i += 1
        if (i == TOTAL)
          l.countDown()
      })
    s.start()
    l.await(60, TimeUnit.SECONDS)
    s.stop()
    i shouldBe TOTAL
    ps.sorted shouldBe ss.sorted
  }

  override def afterAll(): Unit = {
    TestStorageServer.dispose(server)
    onAfterAll()
  }
}
