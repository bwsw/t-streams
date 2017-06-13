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

import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreams.streams.Stream
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode


/**
  * Created by Ivan Kudryavtsev on 23.08.16.
  */
private[tstreams] class Coordinator() {

  var agentAddress: String = null
  var curatorClient: CuratorFramework = null
  var partitions: Set[Int] = null
  var namespace: String = null

  val isInitialized = new AtomicBoolean(false)

  def bootstrap(stream: Stream,
                agentAddress: String,
                partitions: Set[Int]) = this.synchronized {

    if (isInitialized.getAndSet(true))
      throw new IllegalStateException("Failed to initialize object as it's already initialized.")

    this.agentAddress = agentAddress
    this.partitions = partitions
    this.namespace = stream.path
    this.curatorClient = stream.curator
    initializeState()
  }

  /**
    * shuts down coordinator
    */
  def shutdown() = {
    if (!isInitialized.getAndSet(false))
      throw new IllegalStateException("Failed to stop object as it's already stopped.")

    partitions.foreach(partition =>
      curatorClient.delete().forPath(s"$namespace/subscribers/$partition/$agentAddress"))
  }

  /**
    * Try remove this subscriber if it was already created
    *
    */
  private def initializeState(): Unit = {
    partitions.foreach(partition =>
      curatorClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(s"$namespace/subscribers/$partition/$agentAddress"))
  }

}
