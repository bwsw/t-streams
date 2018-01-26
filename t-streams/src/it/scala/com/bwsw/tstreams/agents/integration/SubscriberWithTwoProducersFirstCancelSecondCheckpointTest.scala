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

/**
  * Created by mendelbaum_ma on 08.09.16.
  */

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.consumer.Offset.Newest
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.agents.producer.NewProducerTransactionPolicy
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer


class SubscriberWithTwoProducersFirstCancelSecondCheckpointTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  private lazy val server = TestStorageServer.getNewClean()

  override def beforeAll(): Unit = {
    server
    createNewStream()
  }

  "Subscriber" should "process Integration MixIn checkpoint and cancel correctly" in {

    val bp1 = ListBuffer[Long]()
    val bp2 = ListBuffer[Long]()
    val bs = ListBuffer[Long]()

    val lp2 = new CountDownLatch(1)
    val ls = new CountDownLatch(1)

    val subscriber = f.getSubscriber(name = "ss+2",
      partitions = Set(0),
      offset = Newest,
      useLastOffset = true,
      callback = (_: TransactionOperator, transaction: ConsumerTransaction) => this.synchronized {
        bs.append(transaction.getTransactionID)
        ls.countDown()
      })

    subscriber.start()

    val producer1 = f.getProducer(
      name = "test_producer1",
      partitions = Set(0))

    val producer2 = f.getProducer(
      name = "test_producer2",
      partitions = Set(0))


    val t1 = new Thread(() => {
      val transaction = producer1.newTransaction(policy = NewProducerTransactionPolicy.CheckpointIfOpened)
      lp2.countDown()
      bp1.append(transaction.getTransactionID)
      transaction.send("test")
      transaction.cancel()
    })

    val t2 = new Thread(() => {
      lp2.await()
      val transaction = producer2.newTransaction(policy = NewProducerTransactionPolicy.CheckpointIfOpened)
      bp2.append(transaction.getTransactionID)
      transaction.send("test")
      transaction.checkpoint()
    })

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    ls.await(10, TimeUnit.SECONDS)

    producer1.stop()
    producer2.stop()

    subscriber.stop()

    bs.size shouldBe 1 // Adopted by only one and it is from second
    bp2.head shouldBe bs.head
  }

  override def afterAll(): Unit = {
    TestStorageServer.dispose(server)
    onAfterAll()
  }
}