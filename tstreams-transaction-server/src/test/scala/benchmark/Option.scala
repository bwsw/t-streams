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

package benchmark

import com.bwsw.tstreamstransactionserver.netty.client.{ClientBuilder, InetClientProxy}
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.SingleNodeServerBuilder
import com.bwsw.tstreamstransactionserver.options.ClientOptions.{AuthOptions, ConnectionOptions}
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.{AuthenticationOptions, CommitLogOptions}

private[benchmark] object Options {
  private val key = "pingstation"

  val clientAuthOption =
    AuthOptions(key = key)

  val clientConnectionOption =
    ConnectionOptions(requestTimeoutMs = 200)

  val sharedZookkeeperOption =
    ZookeeperOptions(endpoints = "127.0.0.1:37001,127.0.0.1:37002,127.0.0.1:37003")


  val clientBuilder: ClientBuilder = new ClientBuilder()
    .withAuthOptions(clientAuthOption)
    .withConnectionOptions(clientConnectionOption)
    .withZookeeperOptions(sharedZookkeeperOption)

  val serverAuthenticationOption =
    AuthenticationOptions(key = key)

  val serverBuilder: SingleNodeServerBuilder = new SingleNodeServerBuilder()
    .withAuthenticationOptions(serverAuthenticationOption)
    .withCommitLogOptions(CommitLogOptions(closeDelayMs = 1000))

  def inetClient = {
    new InetClientProxy(
      clientConnectionOption,
      clientAuthOption,
      sharedZookkeeperOption
    )
  }
}
