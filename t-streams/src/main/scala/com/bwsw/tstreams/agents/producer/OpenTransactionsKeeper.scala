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

import com.bwsw.tstreams.agents.producer.NewProducerTransactionPolicy.ProducerPolicy

import scala.collection.mutable


/**
  * Created by Ivan Kudryavtsev on 28.08.16.
  *
  * important: [[scala.collection.mutable.ListBuffer]] was used instead of [[mutable.ArrayBuffer]] within the class
  * but sometimes there was appeared the following exception:
  * Caused by: java.util.NoSuchElementException: head of empty list
  * at scala.collection.immutable.Nil$.head(List.scala:417)
  * at scala.collection.immutable.Nil$.head(List.scala:414)
  * at scala.collection.immutable.List.map(List.scala:276)
  */
class OpenTransactionsKeeper {
  private val openTransactionsMap = mutable.Map[Int, (Long, mutable.Set[ProducerTransaction])]()

  /**
    * Allows to do something with all not closed transactions.
    *
    * @param f
    * @tparam RV
    * @return
    */
  def forallTransactionsDo[RV](f: (Int, ProducerTransaction) => RV): Iterable[RV] = openTransactionsMap.synchronized {
    val res = mutable.ArrayBuffer[RV]()
    openTransactionsMap.keys.foreach(partition =>
      openTransactionsMap.get(partition)
        .foreach(txnSetValue => {
          res ++= txnSetValue._2.map(txn => f(partition, txn))
        }))
    res
  }

  def forPartitionTransactionsDo[RV](partition: Int, f: (ProducerTransaction) => RV): Iterable[RV] = openTransactionsMap.synchronized {
    val res = mutable.ArrayBuffer[RV]()
    openTransactionsMap.get(partition)
      .foreach(txnSetValue => {
        res ++= txnSetValue._2.map(txn => f(txn))
      })
    res
  }

  /**
    * Returns if transaction is in map without checking if it's not closed
    *
    * @param partition
    * @return
    */
  private[tstreams] def getTransactionSetOption(partition: Int) = openTransactionsMap.synchronized {
    openTransactionsMap.get(partition)
  }

  /**
    * Awaits while a transaction for specified partition will be materialized
    *
    * @param partition
    * @param policy
    * @return
    */
  def handlePreviousOpenTransaction(partition: Int, policy: ProducerPolicy): () => Unit = openTransactionsMap.synchronized {
    val transactionSetOption = getTransactionSetOption(partition)

    val allClosed = transactionSetOption.forall(transactionSet => transactionSet._2.forall(_.isClosed))
    if (!allClosed) {
      policy match {
        case NewProducerTransactionPolicy.CheckpointIfOpened =>
          () => transactionSetOption.get._2.foreach(txn => if (!txn.isClosed) txn.checkpoint())

        case NewProducerTransactionPolicy.CancelIfOpened =>
          () => transactionSetOption.get._2.foreach(txn => if (!txn.isClosed) txn.cancel())

        case NewProducerTransactionPolicy.EnqueueIfOpened => () => Unit

        case NewProducerTransactionPolicy.ErrorIfOpened =>
          throw new IllegalStateException(s"Previous transaction was not closed")
      }
    } else {
      () => Unit
    }
  }

  /**
    * Adds new transaction
    *
    * @param partition
    * @param transaction
    * @return
    */
  def put(partition: Int, transaction: ProducerTransaction) = openTransactionsMap.synchronized {
    val transactionSetValueOpt = openTransactionsMap.get(partition)
    if (transactionSetValueOpt.isEmpty) {
      openTransactionsMap.put(partition, (0, mutable.Set[ProducerTransaction](transaction)))
    } else {
      val nextTransactionID = transaction.getTransactionID
      openTransactionsMap
        .put(partition, (nextTransactionID,
          openTransactionsMap.get(partition).get._2 + transaction))
    }
  }

  def remove(partition: Int, transaction: ProducerTransaction) = openTransactionsMap.synchronized {
    openTransactionsMap.get(partition).map(transactionSetValue => transactionSetValue._2.remove(transaction))
  }

  /**
    * Clears all transactions.
    */
  def clear() = openTransactionsMap.synchronized {
    openTransactionsMap.clear()
  }

  def clear(partition: Int) = openTransactionsMap.synchronized {
    openTransactionsMap.remove(partition)
  }

}
