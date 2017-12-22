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

import com.bwsw.tstreams.agents.consumer.subscriber.QueueBuilder.QueueItemType
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.common.FirstFailLockableTaskExecutor
import com.bwsw.tstreamstransactionserver.protocol.TransactionState
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates

import scala.collection.mutable.ListBuffer

private[tstreams] object TransactionFullLoader {
  val EXPECTED_BUT_EMPTY_RESPONSE_RETRY_DELAY = 1000
}

/**
  * Created by Ivan Kudryavtsev on 22.08.16.
  * Loads transactions in full from database if fast loader is unable to load them
  */
private[tstreams] class TransactionFullLoader(partitions: Set[Int],
                                              lastTransactionsMap: ProcessingEngine.LastTransactionStateMapType)
  extends AbstractTransactionLoader {

  /**
    * checks if possible to do full loading
    *
    * @param seq sequence of states which are used to find the last transaction to read from the database.
    * @return
    */
  override def checkIfTransactionLoadingIsPossible(seq: QueueItemType): Boolean = {
    val last = seq.last
    if (last.transactionID <= lastTransactionsMap(last.partition).transactionID) {
      Subscriber.logger.warn(s"Last ID: ${last.transactionID}, In DB ID: ${lastTransactionsMap(last.partition).transactionID}")
      return false
    }
    true
  }

  /**
    * loads transactions to callback
    *
    * @param seq      sequence to load (we need the last one)
    * @param consumer consumer which loads
    * @param executor executor which adds to callback
    * @param callback callback which handles
    */
  override def load(seq: QueueItemType,
                    consumer: TransactionOperator,
                    executor: FirstFailLockableTaskExecutor,
                    callback: Callback): Int = {
    val last = seq.last
    var first = lastTransactionsMap(last.partition).transactionID
    if (Subscriber.logger.isDebugEnabled())
      Subscriber.logger.debug(s"TransactionFullLoader.load: First: $first, last: ${last.transactionID}, ${last.transactionID - first}")
    var transactions = ListBuffer[ConsumerTransaction]()
    var flag = true
    var counter = 0
    var newTransactions = ListBuffer[ConsumerTransaction]()
    while (flag) {
      counter += 1
      if (Subscriber.logger.isDebugEnabled())
        Subscriber.logger.debug(s"Load full begin (partition = ${last.partition}, first = $first, last = ${last.transactionID}, master = ${last.masterID})")

      newTransactions = consumer.getTransactionsFromTo(last.partition, first, last.transactionID)
      transactions ++= newTransactions.filter(transaction => transaction.getState != TransactionStates.Invalid)

      if (Subscriber.logger.isDebugEnabled())
        Subscriber.logger.debug(s"Load full end (partition = ${last.partition}, first = $first, last = ${last.transactionID}, master = ${last.masterID})")

      if (last.masterID > 0 && !last.isNotReliable) {
        if (newTransactions.nonEmpty) {
          if (newTransactions.last.getTransactionID == last.transactionID) {
            if (Subscriber.logger.isDebugEnabled())
              Subscriber.logger.debug(s"Load full completed (partition = ${last.partition}, first = $first, last = ${last.transactionID}, master = ${last.masterID}  )")
            flag = false
          }
          else
            first = newTransactions.last.getTransactionID
        } else {
          /*
          to keep server safe from our activity add small delay
          it happens very rare case - when load full occurred and no data available after response
          so sleep a little bit to give data time to be gathered
          in normal, stationary situation it must not happen.
          */
          Thread.sleep(TransactionFullLoader.EXPECTED_BUT_EMPTY_RESPONSE_RETRY_DELAY)
        }
      } else {
        flag = false
      }
    }

    if (Subscriber.logger.isDebugEnabled())
      Subscriber.logger.debug(s"Series received from the database:  $transactions")

    submitTransactionsToCallback(consumer, executor, callback, last.partition, transactions)

    if (newTransactions.nonEmpty)
      updateLastTransactionMap(partition = last.partition,
        transactionID = newTransactions.last.getTransactionID, masterID = last.masterID,
        orderID = last.orderID, count = newTransactions.last.getCount)
    //      lastTransactionsMap(last.partition) = TransactionState(transactionID = newTransactions.last.getTransactionID,
    //        partition = last.partition, masterID = last.masterID, orderID = last.orderID,
    //        count = newTransactions.last.getCount, status = TransactionState.Status.Checkpointed, ttlMs = -1)
    transactions.size
  }

  private def submitTransactionsToCallback(consumer: TransactionOperator,
                                           executor: FirstFailLockableTaskExecutor,
                                           callback: Callback,
                                           partition: Int,
                                           transactions: ListBuffer[ConsumerTransaction]) = {
    transactions.foreach(elt =>
      executor.submit(s"<CallbackTask#Full>", new ProcessingEngine.CallbackTask(consumer,
        TransactionState(transactionID = elt.getTransactionID,
          partition = partition, masterID = -1, orderID = -1, count = elt.getCount,
          status = TransactionState.Status.Checkpointed, ttlMs = -1), callback)))
  }

  private def updateLastTransactionMap(partition: Int, transactionID: Long, masterID: Int, orderID: Long, count: Int) = {
    lastTransactionsMap(partition) = TransactionState(transactionID = transactionID,
      partition = partition, masterID = masterID, orderID = orderID,
      count = count, status = TransactionState.Status.Checkpointed, ttlMs = -1)
  }

}
