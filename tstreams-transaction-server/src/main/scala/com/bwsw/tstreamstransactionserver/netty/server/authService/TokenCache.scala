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

package com.bwsw.tstreamstransactionserver.netty.server.authService

import java.util.concurrent.TimeUnit

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.TimeUnit

/** Contains valid tokens with it's creation time. Used to handle tokens on a slave.
  *
  * @param tokenTtl token TTL
  * @param timeUnit time unit for tokenTtl
  * @author Pavel Tomskikh
  */
class TokenCache(tokenTtl: Int, timeUnit: TimeUnit = TimeUnit.SECONDS) {

  private val tokenTtlMs = timeUnit.toMillis(tokenTtl)
  private val tokens = TrieMap.empty[Int, Long]

  /** Adds token or updates it's creation time
    *
    * @param token     token
    * @param timestamp creation time
    */
  def add(token: Int, timestamp: Long): Unit =
    tokens += token -> (timestamp + tokenTtlMs)

  /** Removes token
    *
    * @param token token
    */
  def remove(token: Int): Unit = tokens -= token

  /** Removes all expired token
    *
    * @param timestamp current time
    */
  def removeExpired(timestamp: Long): Unit =
    tokens.retain { (_, expirationTime) => expirationTime > timestamp }

  /** Checks that token is valid
    *
    * @param token token
    * @return true, if token is present in cache, false otherwise
    */
  def isValid(token: Int): Boolean = tokens.contains(token)
}
