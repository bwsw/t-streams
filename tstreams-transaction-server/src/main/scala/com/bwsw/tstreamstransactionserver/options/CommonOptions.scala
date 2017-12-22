
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

package com.bwsw.tstreamstransactionserver.options


object CommonOptions {
  val PROPERTY_FILE_NAME = "config"

  /** The options are applied on establishing connection to a ZooKeeper server(cluster).
    *
    * @param endpoints           the socket address(es) of ZooKeeper servers.
    * @param sessionTimeoutMs    the time to wait while trying to re-establish a connection to a ZooKeepers server(s).
    * @param retryDelayMs        delays between retry attempts to establish connection to ZooKeepers server on case of lost connection.
    * @param connectionTimeoutMs the time to wait while trying to establish a connection to a ZooKeepers server(s) on first connection.
    */
  case class ZookeeperOptions(endpoints: String = "127.0.0.1:37001",
                              sessionTimeoutMs: Int = 10000,
                              retryDelayMs: Int = 500,
                              connectionTimeoutMs: Int = 10000)

  /** The options are used for tracing
    *
    * @param enabled  if true, tracing is enabled
    * @param endpoint OpenZipkin server address
    */
  case class TracingOptions(enabled: Boolean = false,
                            endpoint: String = "127.0.0.1:9411")

}
