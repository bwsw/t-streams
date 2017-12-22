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

package com.bwsw.tstreams.common

/**
  * Round robin policy impl of [[PartitionIterationPolicy]]]
  *
  * @param usedPartitions Partitions from which agent will interact
  */

class RoundRobinPartitionIterationPolicy(partitionsCount: Int, usedPartitions: Set[Int])
  extends PartitionIterationPolicy(partitionsCount, usedPartitions = usedPartitions) {

  /**
    * Get next partition to interact and update round value
    *
    * @return Next partition
    */
  override def getNextPartition(): Int = this.synchronized {
    if(currentPos == usedPartitionsList.size) startNewRound()
    val partition = usedPartitionsList(currentPos)
    currentPos += 1
    partition
  }
}



