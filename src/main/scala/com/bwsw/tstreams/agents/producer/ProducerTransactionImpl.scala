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

import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreams.agents.group.ProducerTransactionState
import com.bwsw.tstreamstransactionserver.protocol.TransactionState
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

object ProducerTransactionImpl {
  val logger = LoggerFactory.getLogger(this.getClass)
}

/**
  * Transaction retrieved by BasicProducer.newTransaction method
  *
  * @param partition     Concrete partition for saving this transaction
  * @param producer      Producer class which was invoked newTransaction method
  * @param transactionID ID for this transaction
  */
class ProducerTransactionImpl(partition: Int,
                              transactionID: Long,
                              producer: Producer) extends ProducerTransaction {

  private val data = new ProducerTransactionData(this, producer.stream.ttl, producer.stream.client)
  private val isTransactionClosed = new AtomicBoolean(false)

  /**
    * State indicator of the transaction
    *
    * @return Closed transaction or not
    */
  def isClosed = isTransactionClosed.get

  /**
    * BasicProducerTransaction logger for logging
    */
  ProducerTransactionImpl.logger.debug(s"Open transaction $getTransactionID for\nstream, partition: ${producer.stream.name}, ${}")

  /**
    *
    */
  def markAsClosed() = isTransactionClosed.set(true)

  /**
    * Return transaction partition
    */
  def getPartition: Int = partition


  override def toString: String = s"producer.Transaction(ID=$transactionID, partition=$partition, count=$getDataItemsCount)"

  /**
    * Return transaction ID
    */
  def getTransactionID: Long = transactionID

  /**
    * Return current transaction amount of data
    */
  def getDataItemsCount = data.lastOffset + data.items.size

  /**
    * Returns Transaction owner
    */
  def getProducer = producer

  /**
    * All inserts (can be async) in storage (must be waited before closing this transaction)
    */
  private var jobs = ListBuffer[() => Unit]()


  /**
    * Send data to storage
    *
    * @param obj some user object
    */
  def send(obj: Array[Byte]): ProducerTransaction = this.synchronized {
    producer.checkStopped()
    producer.checkUpdateFailure()

    if (isTransactionClosed.get())
      throw new IllegalStateException(s"Transaction $transactionID is closed. New data items are prohibited.")

    val number = data.put(obj)
    val job = {
      if (number % producer.producerOptions.batchSize == 0) {
        if (ProducerTransactionImpl.logger.isDebugEnabled) {
          ProducerTransactionImpl.logger.debug(s"call data save $number / ${producer.producerOptions.batchSize}")
        }
        data.save()
      }
      else {
        null
      }
    }
    if (job != null) jobs += job
    this
  }

  def send(string: String): ProducerTransaction = send(string.getBytes())

  /**
    * Does actual send of the data that is not sent yet
    */
  def finalizeDataSend(): Unit = {
    if (isTransactionClosed.getAndSet(true))
      throw new IllegalStateException(s"Transaction $transactionID is already closed. Further operations are denied.")

    val job = data.save()
    if (job != null) jobs += job
  }

  private[tstreams] def notifyCancelEvent() = {
    val msg = TransactionState(transactionID = transactionID,
      ttlMs = -1,
      status = TransactionState.Status.Cancelled,
      partition = partition,
      masterID = 0,
      orderID = -1,
      count = 0)
    producer.publish(msg, producer.stream.client.authenticationKey)
  }

  private def cancelTransaction() = {
    producer.stream.client.putTransactionSync(getCancelInfoAndClose.get)
    notifyCancelEvent()
  }


  /**
    * Canceling current transaction
    */
  def cancel(): Unit = this.synchronized {
    producer.checkStopped()
    producer.checkUpdateFailure()
    producer.openTransactions.remove(partition, this)
    cancelTransaction()
  }

  /**
    * Submit transaction(transaction will be available by consumer only after closing)
    */
  def checkpoint(): Unit = this.synchronized {
    producer.checkStopped()
    producer.checkUpdateFailure()

    if (isTransactionClosed.get())
      throw new IllegalStateException(s"Transaction $transactionID is closed. New data items are prohibited.")

    producer.openTransactions.remove(partition, this)

    if (getDataItemsCount > 0) {
      jobs.foreach(x => x())

      if (ProducerTransactionImpl.logger.isDebugEnabled) {
        ProducerTransactionImpl.logger.debug("[START PRE CHECKPOINT PARTITION_{}] ts={}", partition, transactionID.toString)
      }

      val transactionRecord = new RPCProducerTransaction(producer.stream.id, partition, transactionID,
        TransactionStates.Checkpointed, getDataItemsCount, producer.stream.ttl)

      val availTime = producer.checkUpdateFailure()
      producer.stream.client.putTransactionWithDataSync(transactionRecord, data.items, data.lastOffset, availTime)

      Try(producer.checkUpdateFailure()) match {
        case Success(_) =>
        case Failure(exception) =>
          ProducerTransactionImpl.logger.error(s"Detected highly possible transaction TTL overrun for transaction " +
            s"$transactionID at $partition of ${producer.stream.name}[${producer.stream.id}]")
          throw exception
      }

      if (ProducerTransactionImpl.logger.isDebugEnabled) {
        ProducerTransactionImpl.logger.debug("[COMMIT PARTITION_{}] ts={}", partition, transactionID.toString)
      }

      producer.notifyService.submit(s"NotifyTask-Part[$partition]-Txn[$transactionID]", () =>
        producer.publish(TransactionState(
          transactionID = transactionID,
          ttlMs = -1,
          status = TransactionState.Status.Checkpointed,
          partition = partition,
          masterID = 0,
          orderID = -1,
          count = getDataItemsCount), producer.stream.client.authenticationKey))

      if (ProducerTransactionImpl.logger.isDebugEnabled) {
        ProducerTransactionImpl.logger.debug("[FINAL CHECKPOINT PARTITION_{}] ts={}", partition, transactionID.toString)
      }
    }
    else {
      cancelTransaction()
    }

    isTransactionClosed.set(true)
  }

  /**
    *
    * @return
    */
  private[tstreams] def getCancelInfoAndClose: Option[RPCProducerTransaction] = this.synchronized {
    if (ProducerTransactionImpl.logger.isDebugEnabled) {
      ProducerTransactionImpl.logger.debug("Cancel info request for Transaction {}, partition: {}", transactionID, partition)
    }
    val res = if(isClosed) {
      None
    } else {
      Some(new RPCProducerTransaction(producer.stream.id, partition, transactionID, TransactionStates.Cancel, 0, -1L))
    }
    isTransactionClosed.set(true)
    res
  }

  /**
    *
    * @return
    */
  private[tstreams] def getUpdateInfo: Option[RPCProducerTransaction] = {
    if (ProducerTransactionImpl.logger.isDebugEnabled) {
      ProducerTransactionImpl.logger.debug("Update info request for Transaction {}, partition: {}", transactionID, partition)
    }
    if(isClosed) {
      None
    } else {
      Some(new RPCProducerTransaction(producer.stream.id, partition, transactionID, TransactionStates.Updated, -1, producer.producerOptions.transactionTtlMs))
    }
  }

  private[tstreams] def notifyUpdate() = {
    // atomically check state and launch update process
    if(!isClosed) {
      producer.notifyService.submit(s"NotifyTask-Part[$partition]-Txn[$transactionID]", () =>
        producer.publish(TransactionState(
          transactionID = transactionID,
          authKey = producer.stream.client.authenticationKey,
          ttlMs = producer.producerOptions.transactionTtlMs,
          status = TransactionState.Status.Updated,
          partition = partition,
          masterID = 0,
          orderID = -1,
          count = 0), producer.stream.client.authenticationKey))
    }
  }

  def getStateInfo(status: TransactionState.Status): ProducerTransactionState = {

    val event = TransactionState(
      transactionID = getTransactionID,
      authKey = producer.stream.client.authenticationKey,
      ttlMs = -1,
      status = status,
      partition = partition,
      masterID = 0,
      orderID = -1,
      count = getDataItemsCount)

    ProducerTransactionState(transactionRef = this,
      agent = producer,
      event = event,
      streamID = producer.stream.id,
      partition = partition,
      transaction = getTransactionID,
      totalCnt = getDataItemsCount,
      ttl = producer.stream.ttl)
  }

}