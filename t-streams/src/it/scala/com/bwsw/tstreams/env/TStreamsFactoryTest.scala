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

package com.bwsw.tstreams.env

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.testutils.TestStorageServer

/**
  * Created by Ivan Kudryavtsev on 23.07.16.
  */

import com.bwsw.tstreams.testutils.TestUtils
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class TStreamsFactoryTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  f.setProperty(ConfigurationOptions.Stream.ttlSec, 60 * 10)

  private val server = TestStorageServer.getNewClean()
  private val storageClient = f.getStorageClient()
  storageClient.createStream("test-stream", 2, 24 * 3600, "")

  "If copied" should "contain same data" in {
    val n1 = f.copy()
    n1.setProperty(ConfigurationOptions.Stream.name, "cloned-stream")
    val n2 = n1.copy()
    n2.getProperty(ConfigurationOptions.Stream.name) shouldBe "cloned-stream"
  }

  "If locked" should "raise IllegalStateException exception" in {
    val n1 = f.copy()
    n1.lock()
    val res = try {
      n1.setProperty(ConfigurationOptions.Stream.name, "cloned-stream")
      false
    } catch {
      case _: IllegalStateException =>
        true
    }
    res shouldBe true
  }

  "UniversalFactory.getProducer" should "return producer object" in {
    val p = f.getProducer(
      name = "test-producer-1",
      partitions = Set(0))

    p != null shouldEqual true

    p.stop()

  }

  "UniversalFactory.getConsumer" should "return consumer object" in {
    val c = f.getConsumer(
      name = "test-consumer-1",
      partitions = Set(0),
      offset = Oldest)

    c.start()
    c.stop()

    c != null shouldEqual true

  }

  "UniversalFactory.getSubscriber" should "return subscriber object" in {

    val sub = f.getSubscriber(
      name = "test-subscriber",
      partitions = Set(0),
      offset = Oldest,
      callback = (_: TransactionOperator, _: ConsumerTransaction) => {})


    sub != null shouldEqual true

    sub.start()
    sub.stop()
  }

  override def afterAll(): Unit = {
    storageClient.shutdown()
    f.dumpStorageClients()
    TestStorageServer.dispose(server)
    super.afterAll()
  }

}
