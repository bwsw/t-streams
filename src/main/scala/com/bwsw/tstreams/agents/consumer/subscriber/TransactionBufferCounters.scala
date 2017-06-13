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

import java.util.concurrent.atomic.AtomicLong

import com.bwsw.tstreamstransactionserver.protocol.TransactionState

/**
  * Counts events which are delivered to transaction buffer v2
  *
  * @param openEvents
  * @param cancelEvents
  * @param updateEvents
  * @param checkpointEvents
  */
private[tstreams] case class TransactionBufferCounters(openEvents: AtomicLong = new AtomicLong(0),
                                     cancelEvents: AtomicLong = new AtomicLong(0),
                                     updateEvents: AtomicLong = new AtomicLong(0),
                                     checkpointEvents: AtomicLong = new AtomicLong(0),
                                     instantEvents: AtomicLong = new AtomicLong(0)) {
  def dump(partition: Int): Unit = {
    Subscriber.logger.info(s"Partitions $partition - Open Events received: ${openEvents.get()}")
    Subscriber.logger.info(s"Partitions $partition - Cancel Events received: ${cancelEvents.get()}")
    Subscriber.logger.info(s"Partitions $partition - Update Events received: ${updateEvents.get()}")
    Subscriber.logger.info(s"Partitions $partition - Checkpoint Events received: ${checkpointEvents.get()}")
    Subscriber.logger.info(s"Partitions $partition - Instant Events received: ${instantEvents.get()}")
  }


  private[subscriber] def updateStateCounters(state: TransactionState) =
    state.status match {
      case TransactionState.Status.Opened => openEvents.incrementAndGet()
      case TransactionState.Status.Cancelled => cancelEvents.incrementAndGet()
      case TransactionState.Status.Updated => updateEvents.incrementAndGet()
      case TransactionState.Status.Checkpointed => checkpointEvents.incrementAndGet()
      case TransactionState.Status.Instant => instantEvents.incrementAndGet()
      case _ =>
    }
}