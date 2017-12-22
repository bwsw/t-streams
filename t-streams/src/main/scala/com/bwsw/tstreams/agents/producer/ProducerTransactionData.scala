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

package com.bwsw.tstreams.agents.producer

import com.bwsw.tstreams.storage.StorageClient

import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.duration._


/**
  * Created by Ivan Kudryavtsev on 15.08.16.
  */
class ProducerTransactionData(transaction: ProducerTransactionImpl, ttl: Long, storageClient: StorageClient) {
  private[tstreams] var items = ListBuffer[Array[Byte]]()
  private[tstreams] var lastOffset: Int = 0

  private val streamID = transaction.getProducer.stream.id

  def put(elt: Array[Byte]): Int = this.synchronized {
    items += elt
    return items.size
  }

  def save(): () => Unit = this.synchronized {
    val job = storageClient.putTransactionData(streamID, transaction.getPartition, transaction.getTransactionID, items, lastOffset)

    if (Producer.logger.isDebugEnabled()) {
      Producer.logger.debug(s"putTransactionData($streamID, ${transaction.getPartition}, ${transaction.getTransactionID}, $items, $lastOffset)")
    }

    lastOffset += items.size
    items = ListBuffer[Array[Byte]]()
    () => Await.result(job, 1.minute)
  }
}
