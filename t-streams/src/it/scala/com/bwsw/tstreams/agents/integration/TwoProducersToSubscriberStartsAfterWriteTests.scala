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
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer

/**
  * Created by Ivan Kudryavtsev on 07.09.16.
  */
class TwoProducersToSubscriberStartsAfterWriteTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  private lazy val server = TestStorageServer.getNewClean()


  override def beforeAll(): Unit = {
    server
    createNewStream()
  }

  val TRANSACTIONS_COUNT = 1000
  "Two producers" should s"send $TRANSACTIONS_COUNT transactions each, subscriber should receives " +
    s"${2 * TRANSACTIONS_COUNT} when started after." in {

    val bp = ListBuffer[Long]()
    val bs = ListBuffer[Long]()

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
      offset = Oldest,
      useLastOffset = true,
      callback = (_: TransactionOperator, transaction: ConsumerTransaction) => this.synchronized {
        bs.append(transaction.getTransactionID)
        if (bs.size == 2 * TRANSACTIONS_COUNT) {
          ls.countDown()
        }
      })

    val t1 = new Thread(() => {
      lp2.countDown()
      for (_ <- 0 until TRANSACTIONS_COUNT) {
        val t = producer1.newTransaction(policy = NewProducerTransactionPolicy.CheckpointIfOpened)
        t.send("test")
        t.checkpoint()

        bp.synchronized {
          bp.append(t.getTransactionID)
        }
      }
    })
    val t2 = new Thread(() => {
      lp2.await()
      for (_ <- 0 until TRANSACTIONS_COUNT) {
        val t = producer2.newTransaction(policy = NewProducerTransactionPolicy.CheckpointIfOpened)
        t.send("test")
        t.checkpoint()

        bp.synchronized {
          bp.append(t.getTransactionID)
        }
      }
    })


    t1.start()
    t2.start()

    t1.join()
    t2.join()

    s.start()
    ls.await(60, TimeUnit.SECONDS)
    producer1.stop()
    producer2.stop()
    s.stop()
    bp.sorted shouldBe bs.sorted
    bs.size shouldBe 2 * TRANSACTIONS_COUNT
  }


  override def afterAll(): Unit = {
    TestStorageServer.dispose(server)
    onAfterAll()
  }
}