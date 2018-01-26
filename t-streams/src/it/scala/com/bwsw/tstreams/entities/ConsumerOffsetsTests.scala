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

package com.bwsw.tstreams.entities

import com.bwsw.tstreams.testutils.{IncreasingGenerator, TestStorageServer, TestUtils}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}


class ConsumerOffsetsTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  private val server = TestStorageServer.getNewClean()
  private val storageClient = f.getStorageClient()

  "ConsumerEntity.saveSingleOffset() ConsumerEntity.exist() ConsumerEntity.getOffset()" should "create new consumer " +
    "with particular offset, then check consumer existence, then get this consumer offset" in {
    val consumerName = getRandomString
    val streamName = getRandomString
    val stream = storageClient.createStream(streamName, 1, 24 * 3600, "")
    val partition = 1
    val expectedOffset: Long = IncreasingGenerator.get
    storageClient.saveConsumerOffset(consumerName, stream.id, partition, expectedOffset)

    Thread.sleep(500) // wait while server handle it.

    val checkOffsetExist: Boolean = storageClient.checkConsumerOffsetExists(consumerName, stream.id, partition)
    checkOffsetExist shouldBe true
    val receivedOffset: Long = storageClient.getLastSavedConsumerOffset(consumerName, stream.id, partition)
    receivedOffset shouldBe expectedOffset
  }

  "ConsumerEntity.exist()" should "return false if consumer not exist" in {
    val consumerName = getRandomString
    val streamName = getRandomString
    val stream = storageClient.createStream(streamName, 1, 24 * 3600, "test")
    val partition = 1
    storageClient.checkConsumerOffsetExists(consumerName, stream.id, partition) shouldEqual false
  }

  "ConsumerEntity.getOffset()" should "return -1 if consumer offset does not exist" in {
    val consumerName = getRandomString
    val streamName = getRandomString
    val stream = storageClient.createStream(streamName, 1, 24 * 3600, "")
    val partition = 1
    storageClient.getLastSavedConsumerOffset(consumerName, stream.id, partition) shouldBe -1
  }

  "ConsumerEntity.saveBatchOffset(); ConsumerEntity.getOffset()" should "create new consumer with particular offsets and " +
    "then validate this consumer offsets" in {
    val PARTITIONS_COUNT = 100
    val consumerName = getRandomString
    val streamName = getRandomString
    val stream = storageClient.createStream(streamName, 1, 24 * 3600, "")

    val offsets = scala.collection.mutable.Map[Int, Long]()
    for (i <- 0 until PARTITIONS_COUNT)
      offsets(i) = IncreasingGenerator.get

    storageClient.saveConsumerOffsetBatch(consumerName, stream.id, offsets)

    Thread.sleep(500) // wait while server handle it.

    for (i <- 0 until PARTITIONS_COUNT)
      storageClient.getLastSavedConsumerOffset(consumerName, stream.id, i) shouldBe offsets(i)
  }

  override def afterAll(): Unit = {
    storageClient.shutdown()
    TestStorageServer.dispose(server)
    onAfterAll()
  }
}
