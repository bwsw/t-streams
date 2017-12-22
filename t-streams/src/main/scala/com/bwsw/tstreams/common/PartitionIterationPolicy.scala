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
  * Basic interface for policies
  */
abstract class PartitionIterationPolicy(partitionsCount: Int, usedPartitions: Set[Int]) {

  protected val usedPartitionsList = usedPartitions.toList
  @volatile protected var currentPos = 0

  if (usedPartitionsList.isEmpty)
    throw new IllegalArgumentException("UsedPartitions can't be empty")

  if(!usedPartitionsList.forall(partition => partition >= 0 && partition < partitionsCount))
    throw new IllegalArgumentException(s"Invalid partition found among $usedPartitionsList")

  def getNextPartition: Int

  protected[tstreams] def getCurrentPartition: Int = this.synchronized {
    usedPartitionsList(currentPos)
  }

  protected[tstreams] def startNewRound() = {
    currentPos = 0
  }

  def getUsedPartitions = usedPartitions
}
