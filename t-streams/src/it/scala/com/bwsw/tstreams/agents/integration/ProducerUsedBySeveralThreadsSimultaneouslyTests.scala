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

import com.bwsw.tstreams.agents.producer._
import com.bwsw.tstreams.testutils._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer


class ProducerUsedBySeveralThreadsSimultaneouslyTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  val PARTITIONS_COUNT = 4
  val COUNT = 10000
  val THREADS = 8

  private lazy val srv = TestStorageServer.getNewClean()
  private lazy val producer = f.getProducer(
    name = "test_producer",
    partitions = (0 until PARTITIONS_COUNT).toSet)

  override def beforeAll(): Unit = {
    srv
    createNewStream(partitions = PARTITIONS_COUNT)
  }

  "Producer" should "work correctly if two different threads uses different partitions (mixed partitions)" in {
    val l = new CountDownLatch(THREADS)
    val producerAccumulator = new ListBuffer[Long]()

    val threads = (0 until THREADS).map(_ => new Thread(() => {
      (0 until COUNT)
        .foreach(_ => {
          val t = producer.newTransaction(NewProducerTransactionPolicy.EnqueueIfOpened)
          t.send("data")
          t.checkpoint()
          producerAccumulator.synchronized {
            producerAccumulator.append(t.getTransactionID)
          }
        })
      l.countDown()
    }))

    threads.foreach(_.start())
    l.await()
    threads.foreach(_.join())
    producerAccumulator.size shouldBe COUNT * THREADS

  }

  it should "work correctly if two different threads uses different partitions (isolated partitions)" in {
    val l = new CountDownLatch(PARTITIONS_COUNT)
    val producerAccumulator = new ListBuffer[Long]()

    val threads = (0 until PARTITIONS_COUNT).map(partition => new Thread(() => {
      (0 until COUNT)
        .foreach(_ => {
          val t = producer.newTransaction(NewProducerTransactionPolicy.CheckpointIfOpened, partition)
          t.send("data")
          t.checkpoint()
          producerAccumulator.synchronized {
            producerAccumulator.append(t.getTransactionID)
          }
        })
      l.countDown()
    }))

    threads.foreach(_.start())
    l.await()
    threads.foreach(_.join())
    producerAccumulator.size shouldBe COUNT * PARTITIONS_COUNT
  }


  override def afterAll(): Unit = {
    producer.stop()
    TestStorageServer.dispose(srv)
    onAfterAll()
  }
}
