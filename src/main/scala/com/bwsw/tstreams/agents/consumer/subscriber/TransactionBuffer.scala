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


import com.bwsw.tstreamstransactionserver.protocol.TransactionState

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

private[tstreams] object TransactionBuffer {
  val MAX_POST_CHECKPOINT_WAIT = 2000
}

/**
  * Created by ivan on 15.09.16.
  */
private[tstreams] class TransactionBuffer(queue: QueueBuilder.QueueType, transactionQueueMaxLengthThreshold: Int = 1000) {

  val counters = TransactionBufferCounters()
  var lastTransaction = 0L

  private val stateList = ListBuffer[Long]()
  private val stateMap = mutable.HashMap[Long, TransactionState]()

  def getQueue = queue

  def getState(id: Long): Option[TransactionState] = this.synchronized(stateMap.get(id))

  def getSize = stateList.size

  def update(update: TransactionState): Unit = this.synchronized {
    counters.updateStateCounters(update)

    if (update.status == TransactionState.Status.Opened) {
      Try(checkOpenStateIsValid(update)) match {
        case Success(_) =>
          lastTransaction = update.transactionID
        case Failure(exc) => return
      }
    }

    if (stateMap.contains(update.transactionID))
      transitState(update)
    else
      initializeState(update)
  }

  private def checkOpenStateIsValid(update: TransactionState) = {
    // avoid transactions which are delayed
    if (lastTransaction != 0L && update.transactionID <= lastTransaction) {
      Subscriber.logger.warn(s"Unexpected transaction comparison result ${update.transactionID} vs $lastTransaction detected.")
      throw new IllegalStateException("State $update is invalid.")
    }
  }

  private def initializeState(update: TransactionState) = {
    update.status match {
      case TransactionState.Status.Opened =>
        val upd = update.withTtlMs(System.currentTimeMillis() + update.ttlMs)
        stateMap(update.transactionID) = upd
        stateList.append(upd.transactionID)
      case TransactionState.Status.Instant =>
        val upd = update.withStatus(TransactionState.Status.Checkpointed).withTtlMs(Long.MaxValue)
        stateMap(update.transactionID) = upd
        stateList.append(upd.transactionID)
      case _ =>
    }
  }

  private def transitState(update: TransactionState) = {
    val storedState = stateMap(update.transactionID)
    val orderID = storedState.orderID

    stateMap(update.transactionID) = storedState.withMasterID(update.masterID)

    /*
    * state switching system (almost finite automate)
    * */
    (storedState.status, update.status) match {

      case (TransactionState.Status.Opened, TransactionState.Status.Updated) =>
        stateMap(update.transactionID) = storedState
          .withOrderID(orderID)
          .withStatus(TransactionState.Status.Opened)
          .withTtlMs(System.currentTimeMillis() + update.ttlMs)

      case (TransactionState.Status.Opened, TransactionState.Status.Cancelled) =>
        stateMap(update.transactionID) = storedState
          .withStatus(TransactionState.Status.Invalid)
          .withTtlMs(0L)
          .withOrderID(orderID)

      case (TransactionState.Status.Opened, TransactionState.Status.Checkpointed) =>
        stateMap(update.transactionID) = storedState
          .withOrderID(orderID)
          .withStatus(TransactionState.Status.Checkpointed)
          .withCount(update.count)
          .withTtlMs(Long.MaxValue)

      case (_, _) =>
        Subscriber.logger.warn(s"Transaction update $update switched from ${storedState.status} to ${update.status} which is incorrect. " +
          s"It might be that we cleared StateList because it's size has became greater than $transactionQueueMaxLengthThreshold. Try to find clearing notification before.")
    }
  }

  def signalCompleteTransactions(): Unit = this.synchronized {

    signalCheckpointedAndInvalidTransactions()

    if (transactionQueueMaxLengthThreshold <= stateList.size) {
      Subscriber.logger.warn(s"Transaction StateList achieved ${stateList.size} items. The threshold is $transactionQueueMaxLengthThreshold items. Clear it to protect the memory. " +
        "It seems that the user part handles transactions slower than producers feed me.")
      stateList.clear()
      stateMap.clear()
      return
    }

    signalTimedOutTransactions()
  }

  private def signalTimedOutTransactions() = {
    val time = System.currentTimeMillis()

    val meetTimeoutAndInvalid = stateList.takeWhile(ts => stateMap(ts).ttlMs < time)

    if (meetTimeoutAndInvalid.nonEmpty) {
      stateList.remove(0, meetTimeoutAndInvalid.size)
      meetTimeoutAndInvalid.foreach(ts => stateMap.remove(ts))
    }
  }

  private def signalCheckpointedAndInvalidTransactions() = {
    val meetCheckpoint = stateList.takeWhile(ts => {
      val s = stateMap(ts)
      s.status == TransactionState.Status.Checkpointed || s.status == TransactionState.Status.Invalid
    })

    if (meetCheckpoint.nonEmpty) {
      stateList.remove(0, meetCheckpoint.size)
      queue.put(meetCheckpoint.map(transaction => stateMap(transaction)).toList)
    }

    meetCheckpoint.foreach(ts => stateMap.remove(ts))
  }
}