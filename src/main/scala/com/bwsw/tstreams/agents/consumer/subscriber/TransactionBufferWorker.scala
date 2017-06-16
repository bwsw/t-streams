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

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreams.common.FirstFailLockableTaskExecutor
import com.bwsw.tstreamstransactionserver.protocol.TransactionState

import scala.collection.mutable


/**
  * Created by Ivan Kudryavtsev at 20.08.2016
  */
private[tstreams] class TransactionBufferWorker() {
  private val updateExecutor = new FirstFailLockableTaskExecutor("TransactionBufferWorker-updateExecutor")
  private val transactionBufferMap = mutable.Map[Int, TransactionBuffer]()
  private val isComplete = new AtomicBoolean(false)
  private val idleTrigger = createIdleTrigger()

  idleTrigger.start()

  private def createIdleTrigger() = {
    new Thread(() => {
      try {
        while (!isComplete.get) {
          signalTransactionStateSequences()
          Thread.sleep(TransactionBuffer.MAX_POST_CHECKPOINT_WAIT * 2)
        }
      } catch {
        case _: InterruptedException =>
      }
    })
  }

  def assign(partition: Int, transactionBuffer: TransactionBuffer) = this.synchronized {
    if (!transactionBufferMap.contains(partition)) {
      transactionBufferMap(partition) = transactionBuffer
    }
    else
      throw new IllegalStateException(s"Partition $partition is bound already.")
  }

  def signalTransactionStateSequences() = this.synchronized {
    transactionBufferMap.foreach(kv => kv._2.signalCompleteTransactions())
  }

  def getPartitions() = transactionBufferMap.keySet

  /**
    * submits state to executor for offloaded computation
    *
    * @param transactionState
    */
  def updateTransactionState(transactionState: TransactionState) = {

    updateExecutor.submit(s"<UpdateAndNotifyTask($transactionState)>", () => {
      transactionBufferMap(transactionState.partition)
        .updateTransactionState(transactionState.withMasterID(Math.abs(transactionState.masterID)))

      if (transactionState.status == TransactionState.Status.Checkpointed) {
        transactionBufferMap(transactionState.partition).signalCompleteTransactions()
      }
    })

  }

  /**
    * stops executor
    */
  def stop() = {
    isComplete.set(true)
    idleTrigger.interrupt()
    idleTrigger.join()
    updateExecutor.shutdownOrDie(Subscriber.SHUTDOWN_WAIT_MAX_SECONDS, TimeUnit.SECONDS)
    transactionBufferMap.foreach(kv => kv._2.counters.dump(kv._1))
    transactionBufferMap.clear()
  }
}
