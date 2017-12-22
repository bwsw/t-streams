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
package com.bwsw.tstreamstransactionserver.netty.client.zk

import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.exception.Throwable.ZkNoConnectionException
import org.apache.curator.RetryPolicy
import org.apache.curator.framework.state.{ConnectionState, ConnectionStateListener}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.slf4j.LoggerFactory


object ZKClient {
  def addConnectionListener(client: CuratorFramework,
                            onConnectionStateChangeDo: ConnectionState => Unit): ConnectionStateListener = {
    val listener = createListener(
      client,
      onConnectionStateChangeDo
    )

    client.getConnectionStateListenable
      .addListener(listener)

    listener
  }

  private def createListener(client: CuratorFramework,
                             onConnectionStateChangeDo: ConnectionState => Unit) = {
    new ConnectionStateListener {
      override def stateChanged(client: CuratorFramework,
                                newState: ConnectionState): Unit = {
        onConnectionStateChangeDo(newState)
      }
    }
  }

  def removeConnectionListener(client: CuratorFramework,
                               listener: ConnectionStateListener): Unit = {
    client.getConnectionStateListenable
      .removeListener(listener)
  }
}

class ZKClient(endpoints: String,
               sessionTimeoutMillis: Int,
               connectionTimeoutMillis: Int,
               policy: RetryPolicy,
               prefix: String) {

  val client: CuratorFramework = {
    val connection = CuratorFrameworkFactory.builder()
      .sessionTimeoutMs(sessionTimeoutMillis)
      .connectionTimeoutMs(connectionTimeoutMillis)
      .retryPolicy(policy)
      .connectString(endpoints)
      .build()

    connection.start()

    val isConnected = connection
      .blockUntilConnected(
        connectionTimeoutMillis,
        TimeUnit.MILLISECONDS
      )

    if (isConnected) {
      connection
    }
    else
      throw new ZkNoConnectionException(endpoints)
  }
  private val logger = LoggerFactory.getLogger(this.getClass)

  def close(): Unit = {
    client.close()
  }
}

