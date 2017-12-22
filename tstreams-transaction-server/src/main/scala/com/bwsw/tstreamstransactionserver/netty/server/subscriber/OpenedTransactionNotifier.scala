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
package com.bwsw.tstreamstransactionserver.netty.server.subscriber

import java.util.concurrent.atomic.AtomicLong

import com.bwsw.tstreamstransactionserver.netty.RequestMessage
import com.bwsw.tstreamstransactionserver.protocol._
import com.bwsw.tstreamstransactionserver.tracing.ServerTracer.tracer

import scala.util.Random

final class OpenedTransactionNotifier(observer: SubscribersObserver,
                                      notifier: SubscriberNotifier) {
  private val uniqueMasterId =
    Random.nextInt()

  private val counters =
    new java.util.concurrent.ConcurrentHashMap[StreamPartitionUnit, AtomicLong]()

  def notifySubscribers(stream: Int,
                        partition: Int,
                        transactionId: Long,
                        count: Int,
                        status: TransactionState.Status,
                        ttlMs: Long,
                        authKey: String,
                        isNotReliable: Boolean,
                        message: RequestMessage = RequestMessage(-1, 0, -1, Array.empty, -1, -1, isFireAndForgetMethod = false)): Unit = {
    tracer.withTracing(message, name = getClass.getName + ".notifySubscribers") {
      // 1. manage next counter for (stream, part)
      val streamPartitionUnit = StreamPartitionUnit(stream, partition)
      val currentCounter = counters.computeIfAbsent(
        streamPartitionUnit, _ => new AtomicLong(0L)
      ).getAndIncrement()

      // 2. create state (open)
      val transactionState = new TransactionState(
        transactionId,
        partition,
        uniqueMasterId,
        currentCounter,
        count,
        status,
        ttlMs,
        isNotReliable,
        authKey
      )

      // 3. send it via notifier to subscribers
      observer.addSteamPartition(stream, partition)
      val subscribersOpt = observer
        .getStreamPartitionSubscribers(stream, partition)

      subscribersOpt.foreach(subscribers =>
        notifier.broadcast(subscribers, transactionState)
      )
    }
  }
}
