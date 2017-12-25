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

package com.bwsw.tstreams.agents.subscriber

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.consumer.subscriber.TransactionFullLoader
import com.bwsw.tstreams.agents.consumer.{Consumer, ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.common.FirstFailLockableTaskExecutor
import com.bwsw.tstreams.storage.StorageClient
import com.bwsw.tstreams.streams.Stream
import com.bwsw.tstreams.testutils.IncreasingGenerator
import com.bwsw.tstreamstransactionserver.rpc.{TransactionState, TransactionStates}
import org.mockito.Mockito
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class FullLoaderOperatorTestImpl extends TransactionOperator {

  val TOTAL = 10
  val transactions = (0 until TOTAL)
    .map(_ => new ConsumerTransaction(0, IncreasingGenerator.get, 1, TransactionStates.Checkpointed, -1))
    .to[ListBuffer]

  transactions.foreach(_.attach(FullLoaderOperatorTestImpl.Mocks.consumer))


  override def getLastTransaction(partition: Int): Option[ConsumerTransaction] = None

  override def getTransactionById(partition: Int, id: Long): Option[ConsumerTransaction] = None

  override def setStreamPartitionOffset(partition: Int, id: Long): Unit = {}

  override def loadTransactionFromDB(partition: Int, transaction: Long): Option[ConsumerTransaction] = None

  override def getTransactionsFromTo(partition: Int, from: Long, to: Long): ListBuffer[ConsumerTransaction] =
    transactions

  override def checkpoint(): Unit = {}

  override def getPartitions(): Set[Int] = Set[Int](0)

  override def getCurrentOffset(partition: Int) = IncreasingGenerator.get

  override def buildTransactionObject(partition: Int, id: Long, state: TransactionStates, count: Int): Option[ConsumerTransaction] = Some(new ConsumerTransaction(0, IncreasingGenerator.get, 1, TransactionStates.Checkpointed, -1))

  override def getProposedTransactionId(): Long = IncreasingGenerator.get
}

object FullLoaderOperatorTestImpl {
  val authKey = "auth-key"

  object Mocks extends MockitoSugar {
    val storageClient: StorageClient = mock[StorageClient]
    Mockito.when(storageClient.authenticationKey).thenReturn(authKey)

    val stream: Stream = mock[Stream]
    Mockito.when(stream.client).thenReturn(storageClient)

    val consumer: Consumer = mock[Consumer]
    Mockito.when(consumer.stream).thenReturn(stream)
  }

}

trait FullLoaderTestContainer {
  val lastTransactionsMap = mutable.Map[Int, TransactionState]()
  val fullLoader = new TransactionFullLoader(partitions(), lastTransactionsMap)

  def partitions() = Set(0)

  def test()
}


/**
  * Created by Ivan Kudryavtsev on 22.08.16.
  */
class TransactionFullLoaderTests extends FlatSpec with Matchers {

  val authKey = "auth-key"

  it should "load if next state is after prev state by id" in {
    val tc = new FullLoaderTestContainer {
      val partition = 0
      val masterID = 0
      val orderID = 0
      lastTransactionsMap(0) = TransactionState(IncreasingGenerator.get, partition, masterID, orderID, 1, TransactionStates.Checkpointed, -1, authKey)
      val nextTransactionState = List(TransactionState(IncreasingGenerator.get + 1, partition, masterID, orderID + 1, 1, TransactionStates.Checkpointed, -1, authKey))

      override def test(): Unit = {
        fullLoader.checkIfTransactionLoadingIsPossible(nextTransactionState) shouldBe true
      }
    }

    tc.test()
  }

  it should "not load if next state is before prev state by id" in {
    val tc = new FullLoaderTestContainer {
      val partition = 0
      val masterID = 0
      val orderID = 0
      val nextTransactionState = List(TransactionState(IncreasingGenerator.get, partition, masterID, orderID + 1, 1, TransactionStates.Checkpointed, -1, authKey))
      lastTransactionsMap(0) = TransactionState(IncreasingGenerator.get + 1, partition, masterID, orderID, 1, TransactionStates.Checkpointed, -1, authKey)

      override def test(): Unit = {
        fullLoader.checkIfTransactionLoadingIsPossible(nextTransactionState) shouldBe false
      }
    }

    tc.test()
  }

  it should "load all transactions from DB" in {
    val tc = new FullLoaderTestContainer {
      val partition = 0
      val masterID = 0
      val orderID = 0
      lastTransactionsMap(0) = TransactionState(IncreasingGenerator.get, partition, masterID, orderID, 1, TransactionStates.Checkpointed, -1, authKey)
      val fullLoader2 = new TransactionFullLoader(partitions(), lastTransactionsMap)
      val consumerOuter = new FullLoaderOperatorTestImpl()
      val nextTransactionState = List(TransactionState(consumerOuter.transactions.last.getTransactionID, partition, masterID, orderID + 1, 1, TransactionStates.Checkpointed, -1, authKey))

      override def test(): Unit = {
        var ctr: Int = 0
        val l = new CountDownLatch(1)
        fullLoader2.load(nextTransactionState,
          consumerOuter,
          new FirstFailLockableTaskExecutor("lf"),
          (consumer: TransactionOperator, transaction: ConsumerTransaction) => {
            ctr += 1
            if (ctr == consumerOuter.TOTAL)
              l.countDown()
          })
        l.await(1, TimeUnit.SECONDS)
        ctr shouldBe consumerOuter.TOTAL
      }
    }

    tc.test()
  }

  it should "load none transactions from DB" in {
    val tc = new FullLoaderTestContainer {
      val partition = 0
      val masterID = 0
      val orderID = 0
      lastTransactionsMap(0) = TransactionState(IncreasingGenerator.get, partition, masterID, orderID, 1, TransactionStates.Checkpointed, -1, authKey)
      val fullLoader2 = new TransactionFullLoader(partitions(), lastTransactionsMap)
      val consumerOuter = new FullLoaderOperatorTestImpl()
      val nextTransactionState = List(TransactionState(consumerOuter.transactions.last.getTransactionID, partition, masterID, orderID + 1, 1, TransactionStates.Checkpointed, -1, authKey))

      override def test(): Unit = {
        var ctr: Int = 0
        val l = new CountDownLatch(1)
        fullLoader2.load(nextTransactionState,
          consumerOuter,
          new FirstFailLockableTaskExecutor("lf"),
          (consumer: TransactionOperator, transaction: ConsumerTransaction) => {
            ctr += 1
            if (ctr == consumerOuter.TOTAL)
              l.countDown()
          })
        l.await(1, TimeUnit.SECONDS)
        ctr shouldBe consumerOuter.TOTAL
        lastTransactionsMap(0).transactionID shouldBe consumerOuter.transactions.last.getTransactionID
      }
    }

    tc.test()
  }
}
