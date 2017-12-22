
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

object ClientOptions {

  /** The options are applied as filters on establishing connection to a server.
    *
    * @param connectionTimeoutMs      the time to wait while trying to establish a connection to a server.
    * @param requestTimeoutMs         the time to wait a request is completed and response is accepted. On setting option also take into consideration [[com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.TransportOptions]].
    * @param requestTimeoutRetryCount the number of attempts to complete requests before reconnection.
    * @param retryDelayMs             delays between retry attempts.
    * @param threadPool               the number of threads of thread pool to deserialize requests/responses.
    * @param prefix                   the prefix is used for retrieving current master/leader transaction server.
    */
  case class ConnectionOptions(connectionTimeoutMs: Int = 5000,
                               requestTimeoutMs: Int = 5000,
                               requestTimeoutRetryCount: Int = 3,
                               retryDelayMs: Int = 200,
                               threadPool: Int = Runtime.getRuntime.availableProcessors(),
                               prefix: String = "/tts/common/master")

  /** The options are used to validate client requests by a server.
    *
    * @param key the key to authorize.
    */
  case class AuthOptions(key: String = "") extends AnyVal

}
