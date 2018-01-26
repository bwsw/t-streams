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

package com.bwsw.tstreamstransactionserver.netty.server

import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random

class OrderedExecutionContextPoolTest
  extends FlatSpec
    with Matchers {

  private final class RandomStreamIDAndItsPartition(streamIdMax: Int,
                                                    partitionMax: Int) {
    def generate: (Int, Int) = {
      val streamID = Random.nextInt(streamIdMax)
      val partition = Random.nextInt(partitionMax)
      (streamID, partition)
    }
  }


  private def calculatePoolIndex(stream: Int, partition: Int, n: Int) =
    ((stream % n) + (partition % n)) % n


  "OrderedExecutionContextPool" should "should assign tasks as formula requires if pool size is even" in {
    val poolSize = 6
    val orderedExecutionPool = new OrderedExecutionContextPool(poolSize)

    val indexToContext = collection.mutable.Map[Int, String]()

    val rand = new RandomStreamIDAndItsPartition(10000, 10000)
    (0 to 1000).foreach { _ =>
      val (streamID, partition) = rand.generate
      val index = calculatePoolIndex(streamID, partition, poolSize)
      indexToContext.get(index)
        .map(contextName =>
          contextName shouldBe orderedExecutionPool.pool(streamID, partition).toString
        )
        .orElse(
          indexToContext.put(index, orderedExecutionPool.pool(streamID, partition).toString)
        )
    }

    orderedExecutionPool.close()
  }

  it should "should assign tasks as formula requires if pool size is odd" in {
    val poolSize = 13
    val orderedExecutionPool = new OrderedExecutionContextPool(poolSize)

    val indexToContext = collection.mutable.Map[Int, String]()

    val rand = new RandomStreamIDAndItsPartition(10000, 10000)
    (0 to 1000).foreach { _ =>
      val (streamID, partition) = rand.generate
      val index = calculatePoolIndex(streamID, partition, poolSize)
      indexToContext.get(index)
        .map(contextName =>
          contextName shouldBe orderedExecutionPool.pool(streamID, partition).toString
        )
        .orElse(
          indexToContext.put(index, orderedExecutionPool.pool(streamID, partition).toString)
        )
    }

    orderedExecutionPool.close()
  }
}
