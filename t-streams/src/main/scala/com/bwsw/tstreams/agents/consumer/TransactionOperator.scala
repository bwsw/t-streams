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

import com.bwsw.tstreamstransactionserver.rpc.TransactionStates

import scala.collection.mutable.ListBuffer

/**
  * Created by Ivan Kudryavtsev on 22.08.16.
  * Abstract type for Consumer
  */
trait TransactionOperator {
  def getLastTransaction(partition: Int): Option[ConsumerTransaction]

  def getTransactionById(partition: Int, transactionID: Long): Option[ConsumerTransaction]

  def buildTransactionObject(partition: Int, transactionID: Long, state: TransactionStates, count: Int): Option[ConsumerTransaction]

  def setStreamPartitionOffset(partition: Int, transactionID: Long): Unit

  def loadTransactionFromDB(partition: Int, transactionID: Long): Option[ConsumerTransaction]

  def getTransactionsFromTo(partition: Int, from: Long, to: Long): ListBuffer[ConsumerTransaction]

  def checkpoint(): Unit

  def getPartitions: Set[Int]

  def getCurrentOffset(partition: Int): Long

  def getProposedTransactionId: Long
}
