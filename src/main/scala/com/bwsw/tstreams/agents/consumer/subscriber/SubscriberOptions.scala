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

import com.bwsw.tstreams.agents.consumer
import com.bwsw.tstreams.agents.consumer.Offset.IOffset
import com.bwsw.tstreams.agents.consumer.subscriber.QueueBuilder.InMemory
import com.bwsw.tstreams.common.PartitionIterationPolicy

/**
  * Created by Ivan Kudryavtsev on 19.08.16.
  */
/**
  * Class which represents options for subscriber consumer V2
  *
  * @param transactionsPreload
  * @param dataPreload
  * @param readPolicy
  * @param offset
  * @param agentAddress
  * @param zkPrefixPath
  * @param transactionBufferWorkersThreadPoolAmount
  * @param useLastOffset
  */
class SubscriberOptions(val transactionsPreload: Int,
                        val dataPreload: Int,
                        val readPolicy: PartitionIterationPolicy,
                        val offset: IOffset,
                        val useLastOffset: Boolean,
                        val rememberFirstStartOffset: Boolean = true,
                        val agentAddress: String,
                        val zkPrefixPath: String,
                        val transactionBufferWorkersThreadPoolAmount: Int = 1,
                        val processingEngineWorkersThreadAmount: Int = 1,
                        val pollingFrequencyDelayMs: Int = 1000,
                        val transactionQueueMaxLengthThreshold: Int = 10000,
                        val transactionsQueueBuilder: QueueBuilder.Abstract = new InMemory
                            ) {

  def getConsumerOptions() = consumer.ConsumerOptions(
    transactionsPreload = transactionsPreload,
    dataPreload = dataPreload,
    readPolicy = readPolicy,
    offset = offset,
    checkpointAtStart = rememberFirstStartOffset,
    useLastOffset = useLastOffset)

}

