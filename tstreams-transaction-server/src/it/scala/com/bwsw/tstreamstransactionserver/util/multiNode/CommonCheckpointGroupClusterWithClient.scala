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

package com.bwsw.tstreamstransactionserver.util.multiNode

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreamstransactionserver.netty.client.{Client, ClientBuilder}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.{CommonCheckpointGroupServerBuilder, CommonCheckpointGroupTestingServer}
import com.bwsw.tstreamstransactionserver.util.Utils
import com.bwsw.tstreamstransactionserver.util.Utils.{createTtsTempFolder, getRandomPort}

import scala.util.{Failure, Try}

/** Used to run cluster of servers with same ZooKeeper and BookKeeper configurations.
  * Servers can be stopped and started again.
  *
  * @param clientBuilder configured client builder
  * @param serverBuilder configured server buidler
  * @param clusterSize   number of servers
  * @author Pavel Tomskikh
  */
class CommonCheckpointGroupClusterWithClient(val clientBuilder: ClientBuilder,
                                             val serverBuilder: CommonCheckpointGroupServerBuilder,
                                             val clusterSize: Int) {
  val serverBuilders: Array[CommonCheckpointGroupServerBuilder] = Array.fill(clusterSize)(createBuilder)
  private val servers = Array.fill[Option[CommonCheckpointGroupTestingServer]](clusterSize)(None)
  private var _client: Option[Client] = None


  /** Return current instance of a [[Client]]
    *
    * @return current instance of a [[Client]]
    */
  def client: Client =
    _client.getOrElse(throw new IllegalStateException("The client is not created yet."))

  /** Returns i-th server if it is started or None otherwise
    *
    * @param i server's index
    * @return i-th server if it is started or None otherwise
    */
  def get(i: Int): Option[CommonCheckpointGroupTestingServer] = servers(i)

  /** Starts i-th server
    *
    * @param i server's index
    */
  def start(i: Int): Unit = {
    val server = new CommonCheckpointGroupTestingServer(serverBuilders(i))
    val latch = new CountDownLatch(1)
    new Thread(() => {
      server.start(latch.countDown())
    }).start()

    if (!latch.await(5000, TimeUnit.SECONDS))
      throw new IllegalStateException()

    servers(i) = Some(server)
  }

  /** Stops i-th server
    *
    * @param i server's index
    */
  def stop(i: Int): Unit = {
    servers(i).foreach(_.shutdown())
    servers(i) = None
  }

  /** Creates a new instance of a [[Client]] and connects it to a master */
  def startClient(): Unit =
    _client = Some(clientBuilder.build())

  /** Stops currant instance of a [[Client]] */
  def stopClient(): Unit = _client.foreach(_.shutdown())

  /** Does some work with cluster, then stops all running servers and clears temporary folders
    *
    * @param code user code
    */
  def operate(code: () => Unit): Unit = {
    val tried = Try(code())
    clear()

    tried match {
      case Failure(throwable) => throw throwable
      case _ =>
    }
  }

  /** Stops all running servers and clears temporary folders */
  def clear(): Unit = {
    servers.foreach(_.foreach(_.shutdown()))
    _client.foreach(_.shutdown())

    serverBuilders
      .map(_.getStorageOptions)
      .foreach(Utils.deleteDirectories)
  }


  private def createBuilder = {
    serverBuilder
      .withBootstrapOptions(
        serverBuilder.getBootstrapOptions.copy(bindPort = getRandomPort))
      .withServerStorageOptions(
        serverBuilder.getStorageOptions.copy(path = createTtsTempFolder().getPath))
  }
}
