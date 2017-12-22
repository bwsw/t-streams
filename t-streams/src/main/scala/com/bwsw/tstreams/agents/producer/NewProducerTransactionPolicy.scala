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

/**
  * Producer policies for newTransaction method
  */
object NewProducerTransactionPolicy extends Enumeration {
  type ProducerPolicy = Value

  /**
    * If previous transaction was opened it will be checkpointed synchronously
    */
  val CheckpointIfOpened = Value

  /**
    * If previous transaction was opened it will be canceled synchronously
    */
  val CancelIfOpened = Value

  /**
    * If previous transaction was opened exception will be thrown
    */
  val ErrorIfOpened = Value

  /**
    * If a transaction is opened, just append it to the end of the list
    */
  val EnqueueIfOpened = Value

}
