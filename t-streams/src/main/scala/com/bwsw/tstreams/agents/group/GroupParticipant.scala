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

package com.bwsw.tstreams.agents.group

import com.bwsw.tstreams.storage.StorageClient

/**
  * Trait which can be implemented by any producer/consumer to apply group checkpoint
  */
trait GroupParticipant {

  private[tstreams] def getAgentName(): String

  /**
    * Info to commit
    * important: [[List]] was used instead of [[Array]]
    * but sometimes there was appeared the following exception:
    * Caused by: java.util.NoSuchElementException: head of empty list
    * at scala.collection.immutable.Nil$.head(List.scala:417)
    * at scala.collection.immutable.Nil$.head(List.scala:414)
    * at scala.collection.immutable.List.map(List.scala:276)
    */
  private[tstreams] def getStateAndClear(): Array[State]

  private[tstreams] def getStorageClient(): StorageClient

  def isConnected: Boolean = getStorageClient().isConnected
}

/**
  * Agent which sends data into transactions
  */
trait SendingAgent {
  private[agents] def finalizeDataSend(): Unit
}
