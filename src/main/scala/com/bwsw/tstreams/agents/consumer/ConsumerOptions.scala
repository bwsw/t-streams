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

package com.bwsw.tstreams.agents.consumer

import java.net.InetSocketAddress

import com.bwsw.tstreams.agents.consumer.Offset.IOffset
import com.bwsw.tstreams.common.PartitionIterationPolicy

/**
  * @param offset               Offset from which start to read
  * @param useLastOffset        Use or not last offset for specific consumer
  *                             if last offset not exist, offset will be used
  * @param transactionsPreload  Buffer size of preloaded transactions
  * @param dataPreload          Buffer size of preloaded data for each consumed transaction
  * @param readPolicy           Strategy how to read from concrete stream
  */
case class ConsumerOptions(transactionsPreload: Int,
                           dataPreload: Int,
                           readPolicy: PartitionIterationPolicy,
                           offset: IOffset,
                           useLastOffset: Boolean,
                           checkpointAtStart: Boolean) {
  if (transactionsPreload < 1)
    throw new IllegalArgumentException("Incorrect transactionPreload value, should be greater than or equal to one.")

  if (dataPreload < 1)
    throw new IllegalArgumentException("Incorrect transactionDataPreload value, should be greater than or equal to one.")

}

/**
  *
  * @param agentAddress     Subscriber address in network
  * @param zkRootPath       Zk root prefix
  * @param zkHosts          Zk hosts to connect
  * @param zkSessionTimeout Zk session timeout
  * @param threadPoolAmount Thread pool amount which is used by
  *                         [[com.bwsw.tstreams.agents.consumer.subscriber.Subscriber]]]
  *                         by default (threads_amount == used_consumer_partitions)
  */
class SubscriberCoordinationOptions(val agentAddress: String,
                                    val zkRootPath: String,
                                    val zkHosts: List[InetSocketAddress],
                                    val zkSessionTimeout: Int,
                                    val zkConnectionTimeout: Int,
                                    val threadPoolAmount: Int = -1)


