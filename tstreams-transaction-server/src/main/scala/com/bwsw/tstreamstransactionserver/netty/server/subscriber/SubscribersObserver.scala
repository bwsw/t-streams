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

import java.util
import java.util.concurrent.{Executors, TimeUnit}

import com.bwsw.tstreamstransactionserver.netty.server.streamService.{StreamKey, StreamRepository}
import org.apache.curator.framework.CuratorFramework
import org.slf4j.{Logger, LoggerFactory}


private object SubscribersObserver {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
}


private[server] final class SubscribersObserver(curatorClient: CuratorFramework,
                                                streamInteractor: StreamRepository,
                                                updatePeriodMs: Int) {
  private val scheduledExecutor = Executors
    .newSingleThreadScheduledExecutor()
  private val partitionSubscribers =
    new java.util.concurrent.ConcurrentHashMap[StreamPartitionUnit, java.util.List[String]]()
  private val updateSubscribersTask = new Runnable {
    override def run(): Unit = {
      val keys = partitionSubscribers.keys()
      while (keys.hasMoreElements) {
        val streamPartition = keys.nextElement()
        updateSubscribers(
          streamPartition.streamID,
          streamPartition.partition
        )
      }
    }
  }
  @volatile private var isStopped = false

  def addSteamPartition(stream: Int, partition: Int): Unit = {
    val streamPartition = StreamPartitionUnit(stream, partition)
    if (!partitionSubscribers.containsKey(streamPartition)) {
      updateSubscribers(stream, partition)
    }
  }

  //Notify subscribers every 1 sec
  runSubscriberUpdateTask()

  /**
    * Update subscribers on specific partition
    */
  private def updateSubscribers(stream: Int, partition: Int): Unit = {
    val streamPartition = StreamPartitionUnit(stream, partition)
    streamInteractor.get(StreamKey(streamPartition.streamID)).foreach {
      stream =>
        scala.util.Try(
          curatorClient.getChildren
            .forPath(s"${stream.zkPath}/subscribers/${streamPartition.partition}")
        ).map { childrenPaths =>
          if (childrenPaths.isEmpty)
            partitionSubscribers.remove(streamPartition)
          else
            partitionSubscribers.put(streamPartition, childrenPaths)
        }
    }
  }

  def getStreamPartitionSubscribers(stream: Int, partition: Int): Option[util.List[String]] = {
    val streamPartition = StreamPartitionUnit(stream, partition)
    Option(partitionSubscribers.get(streamPartition))
  }

  /**
    * Stop this Subscriber client
    */
  def shutdown(): Unit = {
    if (isStopped) {
      throw new IllegalStateException(
        "Producer->Subscriber notifier was stopped second time."
      )
    } else {
      isStopped = true

      scheduledExecutor.shutdown()
      scala.util.Try(scheduledExecutor.awaitTermination(
        updatePeriodMs,
        TimeUnit.MILLISECONDS
      ))
      partitionSubscribers.clear()
    }
  }

  private def runSubscriberUpdateTask(): Unit = {
    scheduledExecutor.scheduleWithFixedDelay(
      updateSubscribersTask,
      0L,
      updatePeriodMs,
      TimeUnit.MILLISECONDS
    )
  }
}

