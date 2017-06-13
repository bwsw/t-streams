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

import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates

/**
  * Trait to implement to handle incoming messages
  */
trait Callback {
  /**
    * Callback which is called on every closed transaction
    *
    * @param consumer    associated Consumer
    * @param transaction the transaction which currently has delivered
    */
  def onTransaction(consumer: TransactionOperator,
                    transaction: ConsumerTransaction): Unit

  /**
    *
    * @param consumer      consumer object which is associated with the transaction
    * @param partition     partition on which the transaction is
    * @param transactionID transaction ID
    * @param count         amount of data items inside of the transaction
    */
  def onTransactionCall(consumer: TransactionOperator,
                        partition: Int,
                        transactionID: Long,
                        count: Int) = {
    consumer.setStreamPartitionOffset(partition, transactionID)
    consumer.buildTransactionObject(partition, transactionID, TransactionStates.Checkpointed, count)
      .foreach(transaction => onTransaction(consumer, transaction = transaction))
  }
}
