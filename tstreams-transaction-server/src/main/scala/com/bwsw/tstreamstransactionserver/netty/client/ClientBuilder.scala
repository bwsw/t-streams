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

package com.bwsw.tstreamstransactionserver.netty.client

import com.bwsw.tstreamstransactionserver.options.ClientOptions._
import com.bwsw.tstreamstransactionserver.options.CommonOptions.{TracingOptions, ZookeeperOptions}
import org.apache.curator.framework.CuratorFramework


class ClientBuilder private(authOpts: AuthOptions,
                            zookeeperOpts: ZookeeperOptions,
                            connectionOpts: ConnectionOptions,
                            curatorOpt: Option[CuratorFramework],
                            tracingOpts: TracingOptions) {
  private val authOptions = authOpts
  private val zookeeperOptions = zookeeperOpts
  private val connectionOptions = connectionOpts
  private val curator: Option[CuratorFramework] = curatorOpt
  private val tracingOptions = tracingOpts

  def this() =
    this(AuthOptions(), ZookeeperOptions(), ConnectionOptions(), None, TracingOptions())

  def withAuthOptions(authOptions: AuthOptions) =
    new ClientBuilder(authOptions, zookeeperOptions, connectionOptions, curator, tracingOptions)

  def withZookeeperOptions(zookeeperOptions: ZookeeperOptions) =
    new ClientBuilder(authOptions, zookeeperOptions, connectionOptions, curator, tracingOptions)

  def withCuratorConnection(curator: CuratorFramework) =
    new ClientBuilder(authOptions, zookeeperOptions, connectionOptions, Some(curator), tracingOptions)

  def withConnectionOptions(clientOptions: ConnectionOptions) =
    new ClientBuilder(authOptions, zookeeperOptions, clientOptions, curator, tracingOptions)

  def withTracingOptions(tracingOptions: TracingOptions) =
    new ClientBuilder(authOptions, zookeeperOptions, connectionOptions, curator, tracingOptions)

  def build() =
    new Client(connectionOptions, authOptions, zookeeperOptions, curator, tracingOptions)

  def getConnectionOptions: ConnectionOptions = connectionOptions.copy()

  def getZookeeperOptions: ZookeeperOptions = zookeeperOptions.copy()

  def getAuthOptions: AuthOptions = authOptions.copy()

  def getTracingOptions: TracingOptions = tracingOptions
}