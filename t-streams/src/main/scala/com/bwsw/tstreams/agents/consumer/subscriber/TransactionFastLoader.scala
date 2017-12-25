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


import com.bwsw.tstreams.agents.consumer.TransactionOperator
import com.bwsw.tstreams.common.FirstFailLockableTaskExecutor
import com.bwsw.tstreamstransactionserver.rpc.{TransactionState, TransactionStates}

/**
  * Created by Ivan Kudryavtsev on 21.08.16.
  */
private[tstreams] class TransactionFastLoader(partitions: Set[Int],
                                              lastTransactionsMap: ProcessingEngine.LastTransactionStateMapType)
  extends AbstractTransactionLoader {

  /**
    * checks that two items satisfy load fast condition
    *
    * @param e1
    * @param e2
    * @return
    */
  private def compareIfStrictlySequentialFast(e1: TransactionState, e2: TransactionState): Boolean =
    (e1.masterID == e2.masterID) && (e2.orderID - e1.orderID == 1) && partitions.contains(e2.partition)


  /**
    * utility function with compares head and tail of the list to find if they satisfy some condition
    *
    * @param head
    * @param l
    * @return
    */
  private def checkListSeq(head: TransactionState,
                           l: QueueBuilder.QueueItemType,
                           predicate: (TransactionState, TransactionState) => Boolean): Boolean =
    (head, l, predicate) match {
      case (_, Nil, _) => true
      case (_, elt :: tail, _) =>
        predicate(head, elt) && checkListSeq(elt, tail, predicate)
    }

  /**
    * Checks if seq can be load fast without additional calls to database
    *
    * @param seq
    * @return
    */
  override def checkIfTransactionLoadingIsPossible(seq: QueueBuilder.QueueItemType): Boolean = {
    val first = seq.head
    // if there is no last for partition, then no info

    val prev = lastTransactionsMap(first.partition)
    checkListSeq(prev, seq, compareIfStrictlySequentialFast)
  }

  /**
    * allows to load data fast without database calls
    *
    * @param seq
    */
  override def load(seq: QueueBuilder.QueueItemType,
                    consumer: TransactionOperator,
                    executor: FirstFailLockableTaskExecutor,
                    callback: Callback): Int = {

    seq.foreach(elt =>
      if (elt.status == TransactionStates.Checkpointed)
        executor.submit(s"<CallbackTask#Fast>", new ProcessingEngine.CallbackTask(consumer, elt, callback)))

    val last = seq.last
    lastTransactionsMap(last.partition) = last
    seq.size
  }

}
