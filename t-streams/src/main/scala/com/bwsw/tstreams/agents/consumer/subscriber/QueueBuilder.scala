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

package com.bwsw.tstreams.agents.consumer.subscriber

import com.bwsw.tstreams.common.{MemoryQueue, Queue}
import com.bwsw.tstreamstransactionserver.rpc.TransactionState

/**
  * Represents factory which generates queues
  */
private[tstreams] object QueueBuilder {

  type QueueItemType = List[TransactionState]
  type QueueType = Queue[QueueItemType]

  /**
    * Abstract factory
    */
  trait Abstract {
    def generateQueueObject(id: Int): QueueType
  }

  /**
    * InMemory Queues factory
    */
  class InMemory extends Abstract {
    override def generateQueueObject(id: Int): QueueType =
      new MemoryQueue[QueueItemType]()
  }

}