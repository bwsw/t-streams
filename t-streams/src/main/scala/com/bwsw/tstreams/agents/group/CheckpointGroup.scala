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

package com.bwsw.tstreams.agents.group

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreams.agents.producer.Producer
import com.bwsw.tstreams.common.{CommonConstants, FirstFailLockableTaskExecutor}
import com.bwsw.tstreams.storage.StorageClient
import com.bwsw.tstreamstransactionserver.rpc
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._


object CheckpointGroup {
  private val logger = LoggerFactory.getLogger(classOf[CheckpointGroup])
  var SHUTDOWN_WAIT_MAX_SECONDS: Int = CommonConstants.SHUTDOWN_WAIT_MAX_SECONDS
}

/**
  * Base class to creating agent group
  */
class CheckpointGroup private[tstreams](val executors: Int = 1) {
  /**
    * Group of agents (stateInfoList/consumer)
    */
  private val agents = scala.collection.mutable.Map[String, GroupParticipant]()
  private lazy val executorPool = new FirstFailLockableTaskExecutor("CheckpointGroup-Workers", executors)
  private val isStopped = new AtomicBoolean(false)
  private val log = CheckpointGroup.logger


  /**
    * Add new agent in group
    *
    * @param agent Agent ref
    */
  def add(agent: GroupParticipant): CheckpointGroup = this.synchronized {
    checkStopped()
    if (agents.contains(agent.getAgentName())) {
      throw new IllegalArgumentException(
        s"Agent with specified name ${agent.getAgentName()} is already in the group. " +
          "Names of added agents must be unique.")
    }
    agents += ((agent.getAgentName(), agent))

    this
  }

  /**
    * clears group
    */
  def clear(): CheckpointGroup = this.synchronized {
    checkStopped()
    agents.clear()

    this
  }

  /**
    * Remove agent from group
    *
    * @param name Agent name
    */
  def remove(name: String): CheckpointGroup = this.synchronized {
    checkStopped()
    if (agents.remove(name).isEmpty)
      throw new IllegalArgumentException(s"Agent with specified name $name is not in the group.")

    this
  }

  /**
    * Checks if an agent with the name is already inside
    */
  def exists(name: String): Boolean = this.synchronized {
    checkStopped()

    agents.contains(name)
  }

  private def checkUpdateFailure(producers: Seq[State]) = {
    producers.map {
      case ProducerTransactionState(_, agent, _, _) => agent.checkUpdateFailure()
      case _ => 1.minute
    }.min
  }

  /**
    * Group agents commit
    *
    * @param checkpointRequests Info to commit
    *                           (used only for consumers now; stateInfoList is not atomic)
    */
  private def doGroupCheckpoint(storageClient: StorageClient, checkpointRequests: Array[State]): Unit = {
    val producerRequests = ListBuffer[rpc.ProducerTransaction]()
    val consumerRequests = ListBuffer[rpc.ConsumerTransaction]()

    checkpointRequests foreach {
      case ConsumerState(consumerName, stream, partition, offset) =>
        consumerRequests.append(rpc.ConsumerTransaction(stream, partition, offset, consumerName))

      case ProducerTransactionState(_, _, _, rpcTransaction) =>
        producerRequests.append(rpcTransaction)
    }

    //check stateInfoList update timeout
    val availTime = checkUpdateFailure(checkpointRequests)
    storageClient.putTransactions(producerRequests, consumerRequests, availTime)
  }

  private def doGroupCancel(storageClient: StorageClient, checkpointRequests: Array[State]): Unit = {
    val producerRequests = checkpointRequests.flatMap {
      case ConsumerState(_, _, _, _) => Seq.empty
      case ProducerTransactionState(_, _, _, rpcTransaction) =>
        Seq(rpcTransaction.copy(state = TransactionStates.Cancel))
    }

    //check stateInfoList update timeout
    val availTime = checkUpdateFailure(checkpointRequests)
    storageClient.putTransactions(producerRequests, Nil, availTime)
  }

  /**
    * Commit all agents state
    */
  def checkpoint(): Unit = this.synchronized {
    checkStopped()

    if (agents.isEmpty)
      return

    if (log.isDebugEnabled())
      log.debug("Complete send data to storage server for all participants")

    agents.foreach {
      case (_, agent: SendingAgent) => agent.finalizeDataSend()
      case _ =>
    }

    if (log.isDebugEnabled())
      log.debug("Gather checkpoint information from participants")

    // receive from all agents their checkpoint information
    val checkpointRequests = agents
      .values
      .flatMap(_.getStateAndClear())
      .toArray

    if (checkpointRequests.isEmpty)
      return

    if (log.isDebugEnabled()) {
      log.debug(s"CheckpointGroup Info $checkpointRequests\n" + "Do group checkpoint.")
    }
    //assume all agents use the same metadata entity
    doGroupCheckpoint(agents.head._2.getStorageClient(), checkpointRequests)

    if (log.isDebugEnabled()) log.debug("Do publish notifications")

    publishEventForAllProducers(checkpointRequests)

    if (log.isDebugEnabled()) log.debug("End checkpoint")
  }


  def cancel(): Unit = this.synchronized {
    checkStopped()

    if (agents.isEmpty)
      return

    // receive from all agents their checkpoint information
    val cancelStateInfo = agents.values
      .flatMap {
        case producer: Producer => producer.getCancelInfoAndClear()
        case _ => Array.empty[State]
      }
      .toArray

    if (cancelStateInfo.isEmpty)
      return

    if (log.isDebugEnabled()) {
      log.debug(s"CheckpointGroup Info $cancelStateInfo\n" + "Do group cancel.")
    }

    //assume all agents use the same metadata entity
    doGroupCancel(agents.head._2.getStorageClient(), cancelStateInfo)

    if (log.isDebugEnabled()) log.debug("Do publish notifications")

    publishEventForAllProducers(cancelStateInfo)

    if (log.isDebugEnabled()) log.debug("End checkpoint")
  }


  private def publishEventForAllProducers(stateInfoList: Array[State]) = {
    stateInfoList foreach {
      case ProducerTransactionState(_, agent, stateEvent, _) =>
        executorPool.submit("<Event>", () => agent.publish(stateEvent), None)
      case _ =>
    }
  }

  /**
    * Stop group when it's no longer required
    */
  def stop(): Unit = this.synchronized {
    clear()
    if (isStopped.getAndSet(true))
      throw new IllegalStateException("Group is stopped. No longer operations are possible.")
    executorPool.shutdownOrDie(CheckpointGroup.SHUTDOWN_WAIT_MAX_SECONDS, TimeUnit.SECONDS)
  }

  private def checkStopped(): Unit = {
    if (isStopped.get())
      throw new IllegalStateException("Group is stopped. No longer operations are possible.")
  }
}

