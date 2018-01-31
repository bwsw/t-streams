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

import scala.collection.concurrent.TrieMap

/** Contains opened transactions
  *
  * @param tokenCache cache for tokens
  * @author Pavel Tomskikh
  */
class OpenedTransactions(val tokenCache: TokenCache) {

  private val transactions = TrieMap.empty[Long, Int]

  /** Adds transaction with specific token if it is absent and token is valid
    *
    * @param transaction transaction ID
    * @param token       token
    */
  def add(transaction: Long, token: Int): Unit = {
    if (!transactions.contains(transaction) && tokenCache.isValid(token)) {
      transactions += transaction -> token
    }
  }

  /** Removes transaction
    *
    * @param transaction transaction ID
    */
  def remove(transaction: Long): Unit = transactions -= transaction

  /** Checks that transaction is valid
    *
    * @param transaction transaction ID
    * @return true, if transaction is presented and it has valid token, false otherwise
    */
  def isValid(transaction: Long): Boolean = {
    transactions.get(transaction) match {
      case Some(token) if tokenCache.isValid(token) => true
      case Some(_) =>
        remove(transaction)

        false
      case None => false
    }
  }
}


object OpenedTransactions {
  def apply(tokenTtlSec: Int): OpenedTransactions =
    new OpenedTransactions(new TokenCache(tokenTtlSec))
}
