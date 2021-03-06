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

import com.bwsw.tstreams.agents.producer._
import com.bwsw.tstreams.env.ConfigurationOptions
import com.bwsw.tstreams.testutils._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}


class ProducerTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  val PARTITIONS_COUNT = 10

  lazy val srv = TestStorageServer.getNewClean()

  lazy val producer = f.getProducer(
    name = "test_producer",
    partitions = (0 until PARTITIONS_COUNT).toSet)


  override def beforeAll(): Unit = {
    f.setProperty(ConfigurationOptions.Stream.partitionsCount, PARTITIONS_COUNT)
    srv
    createNewStream(partitions = PARTITIONS_COUNT)
  }


  "BasicProducer.newTransaction()" should "return BasicProducerTransaction instance" in {
    val transaction = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)
    transaction.checkpoint()
    transaction.isInstanceOf[ProducerTransactionImpl] shouldEqual true
  }

  "BasicProducer.newTransaction(ProducerPolicies.ErrorIfOpened)" should "throw exception if previous transaction was not closed" in {
    val transaction1 = producer.newTransaction(NewProducerTransactionPolicy.CheckpointIfOpened, 2)
    intercept[IllegalStateException] {
      producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened, 2)
    }
    transaction1.checkpoint()
  }

  "BasicProducer.newTransaction(ProducerPolicies.EnqueueIfOpened)" should "not throw exception if previous transaction was not closed" in {
    val transaction1 = producer.newTransaction(NewProducerTransactionPolicy.EnqueueIfOpened, 3)
    val transaction2 = producer.newTransaction(NewProducerTransactionPolicy.EnqueueIfOpened, 3)
    transaction1.checkpoint()
    producer.getOpenedTransactionsForPartition(3).get.size shouldBe 1
    transaction2.checkpoint()
    producer.getOpenedTransactionsForPartition(3).get.size shouldBe 0
  }

  "BasicProducer.newTransaction(ProducerPolicies.EnqueueIfOpened) and checkpoint" should "not throw exception if previous transaction was not closed" in {
    val transaction1 = producer.newTransaction(NewProducerTransactionPolicy.EnqueueIfOpened, 3)
    val transaction2 = producer.newTransaction(NewProducerTransactionPolicy.EnqueueIfOpened, 3)
    producer.checkpoint()
    transaction1.isClosed shouldBe true
    transaction2.isClosed shouldBe true
  }

  "BasicProducer.newTransaction(CheckpointIfOpen)" should "not throw exception if previous transaction was not closed" in {
    producer.newTransaction(NewProducerTransactionPolicy.CheckpointIfOpened, 2)
    val transaction2 = producer.newTransaction(NewProducerTransactionPolicy.CheckpointIfOpened, 2)
    transaction2.checkpoint()
  }

  "BasicProducer.getTransaction()" should "return transaction reference if it was created or None" in {
    val transaction = producer.newTransaction(NewProducerTransactionPolicy.CheckpointIfOpened, 1)
    val transactionRef = producer.getOpenedTransactionsForPartition(1)
    transaction.checkpoint()
    transactionRef.get.contains(transaction) shouldBe true
  }

  "BasicProducer.instantTransaction" should "work well for reliable delivery" in {
    val data = Seq(new Array[Byte](128))
    producer.instantTransaction(0, data, isReliable = true) > 0 shouldBe true
  }

  "BasicProducer.instantTransaction" should "work well for unreliable delivery" in {
    val data = Seq(new Array[Byte](128))
    producer.instantTransaction(0, data, isReliable = false) == 0 shouldBe true
  }

  "BasicProducer.instantTransaction" should "work and doesn't prevent from correct functioning of regular one" in {
    val regularTransaction = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened, 0)
    regularTransaction.send("test".getBytes)
    val data = Seq(new Array[Byte](128))
    producer.instantTransaction(0, data, isReliable = false) == 0 shouldBe true
    regularTransaction.checkpoint()
  }

  override def afterAll(): Unit = {
    producer.stop()
    TestStorageServer.dispose(srv)
    onAfterAll()
  }
}
