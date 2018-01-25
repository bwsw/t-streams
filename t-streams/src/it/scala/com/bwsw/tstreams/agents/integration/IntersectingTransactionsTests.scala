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

import com.bwsw.tstreams.agents.consumer.Offset.Newest
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.agents.producer.NewProducerTransactionPolicy
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer

/**
  * Created by Mikhail Mendelbaum on 02.09.16.
  */
class IntersectingTransactionsTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  private lazy val server = TestStorageServer.getNewClean()

  override def beforeAll(): Unit = {
    server
    createNewStream()
  }

  "Subscriber" should "handle all transactions produced by two different producers, the first ends first started " in {
    val bp = ListBuffer[Long]()
    val bs = ListBuffer[Long]()
    val lp1 = new CountDownLatch(1)
    val lp2 = new CountDownLatch(1)
    val ls = new CountDownLatch(1)

    val producer1 = f.getProducer(
      name = "test_producer1",
      partitions = Set(0))

    val producer2 = f.getProducer(
      name = "test_producer2",
      partitions = Set(0))

    val s = f.getSubscriber(name = "ss+2",
      partitions = Set(0),
      offset = Newest,
      useLastOffset = true,
      callback = (_: TransactionOperator, transaction: ConsumerTransaction) => this.synchronized {
        bs.append(transaction.getTransactionID)
        if (bs.size == 2) {
          ls.countDown()
        }
      })

    val t1 = new Thread(() => {
      val t = producer1.newTransaction(policy = NewProducerTransactionPolicy.CheckpointIfOpened)
      bp.append(t.getTransactionID)
      lp2.countDown()
      lp1.await()
      t.send("test".getBytes())
      t.checkpoint()
    })

    val t2 = new Thread(() => {
      lp2.await()
      val t = producer2.newTransaction(policy = NewProducerTransactionPolicy.CheckpointIfOpened)
      bp.append(t.getTransactionID)
      t.send("test".getBytes())
      t.checkpoint()
      lp1.countDown()
    })

    s.start()

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    ls.await()

    producer1.stop()
    producer2.stop()
    s.stop()

    bp.head shouldBe bs.head
    bp.tail.head shouldBe bs.tail.head
  }

  override def afterAll(): Unit = {
    TestStorageServer.dispose(server)
    onAfterAll()
  }

}
