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
import com.bwsw.tstreams.agents.producer.NewProducerTransactionPolicy
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Created by ivan on 21.05.17.
  */
class ProducerSubscriberPartitionCheckpointTest  extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  lazy val srv = TestStorageServer.getNewClean()

  override def beforeAll(): Unit = {
    srv
    createNewStream()
  }

  override def afterAll(): Unit = {
    TestStorageServer.dispose(srv)
    onAfterAll()
  }

  "Producer" should "do checkpoint(partition) correctly" in {
    val TOTAL = 300
    val latch = new CountDownLatch(TOTAL)
    var wrongPartition = false

    val producer = f.getProducer(
      name = "test_producer",
      partitions = Set(0, 1, 2))

    val s = f.getSubscriber(name = "subscriber",
      partitions = Set(0, 1, 2),
      offset = Newest,
      useLastOffset = false,
      callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => {
        latch.countDown()
        if(transaction.getPartition > 0)
          wrongPartition = true
      })
    s.start()

    producer.checkpoint(0)

    for (it <- 0 until TOTAL * 3) {
      producer
        .newTransaction(NewProducerTransactionPolicy.EnqueueIfOpened)
        .send("test")
    }
    producer.checkpoint(0).stop()

    latch.await(60, TimeUnit.SECONDS) shouldBe true
    Thread.sleep(1000)
    wrongPartition shouldBe false

    s.stop()
  }
}
