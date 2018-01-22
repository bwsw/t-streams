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

import com.bwsw.tstreams.agents.consumer.Offset.Newest
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer

/**
  * Created by Ivan Kudryavtsev on 14.04.17.
  */
class SubscriberTransactionPartitionDistributionTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  val PARTITIONS_COUNT = 4

  private lazy val server = TestStorageServer.getNewClean()

  override def beforeAll(): Unit = {
    server
    createNewStream(partitions = PARTITIONS_COUNT)
  }

  "Subscriber" should "retrieve transaction from correct partitions" in {
    val TRANSACTION_COUNT = 1000

    val accumulatorBuilder = () => (0 until PARTITIONS_COUNT).map(_ => ListBuffer[Long]()).toArray
    val producerTransactionsAccumulator = accumulatorBuilder()
    val subscriberTransactionsAccumulator = accumulatorBuilder()
    var counter = 0
    val subscriberLatch = new CountDownLatch(1)

    val producer = f.getProducer(
      name = "test_producer",
      partitions = (0 until PARTITIONS_COUNT).toSet)

    val subscriber = f.getSubscriber(name = "sv2",
      partitions = (0 until PARTITIONS_COUNT).toSet,
      offset = Newest,
      useLastOffset = false,
      callback = (_: TransactionOperator, transaction: ConsumerTransaction) => this.synchronized {
        subscriberTransactionsAccumulator(transaction.getPartition).append(transaction.getTransactionID)
        counter += 1
        if (counter == TRANSACTION_COUNT)
          subscriberLatch.countDown()
      })
    subscriber.start()
    (0 until TRANSACTION_COUNT).foreach(_ => {
      val t = producer.newTransaction()
      t.send("test".getBytes())
      t.checkpoint()
      producerTransactionsAccumulator(t.getPartition).append(t.getTransactionID)
    })

    subscriberLatch.await(30, TimeUnit.SECONDS) shouldBe true
    (0 until PARTITIONS_COUNT).foreach(partition =>
      subscriberTransactionsAccumulator(partition) shouldBe producerTransactionsAccumulator(partition))

    subscriber.stop()
    producer.stop()
  }

  override def afterAll(): Unit = {
    onAfterAll()
    TestStorageServer.dispose(server)
  }
}
