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

import org.mockito.ArgumentMatchers.anyInt
import org.mockito.Mockito.when
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random

/** Tests for [[OpenedTransactions]]
  *
  * @author Pavel Tomskikh
  */
class OpenedTransactionsTests extends FlatSpec with Matchers with MockitoSugar {

  private val tokenCache = mock[TokenCache]
  private val token1 = Random.nextInt()
  private val token2 = Random.nextInt()
  private val token3 = Random.nextInt()

  when(tokenCache.isValid(anyInt)).thenReturn(false)
  when(tokenCache.isValid(token1)).thenReturn(true)
  when(tokenCache.isValid(token2)).thenReturn(true)
  when(tokenCache.isValid(token3)).thenReturn(true)


  "OpenedTransactions" should "work properly" in {
    val openedTransactions = new OpenedTransactions(tokenCache)
    val transaction1 = Random.nextLong()
    val transaction2 = Random.nextLong()
    val transaction3 = Random.nextLong()

    openedTransactions.isValid(transaction1) shouldBe false
    openedTransactions.isValid(transaction2) shouldBe false
    openedTransactions.isValid(transaction3) shouldBe false

    openedTransactions.add(transaction1, token1)
    openedTransactions.isValid(transaction1) shouldBe true
    openedTransactions.isValid(transaction2) shouldBe false
    openedTransactions.isValid(transaction3) shouldBe false

    openedTransactions.add(transaction2, token2)
    openedTransactions.isValid(transaction1) shouldBe true
    openedTransactions.isValid(transaction2) shouldBe true
    openedTransactions.isValid(transaction3) shouldBe false

    openedTransactions.add(transaction3, token3)
    openedTransactions.isValid(transaction1) shouldBe true
    openedTransactions.isValid(transaction2) shouldBe true
    openedTransactions.isValid(transaction3) shouldBe true

    when(tokenCache.isValid(token2)).thenReturn(false)

    openedTransactions.isValid(transaction1) shouldBe true
    openedTransactions.isValid(transaction2) shouldBe false
    openedTransactions.isValid(transaction3) shouldBe true

    openedTransactions.remove(transaction1)
    openedTransactions.isValid(transaction1) shouldBe false
    openedTransactions.isValid(transaction2) shouldBe false
    openedTransactions.isValid(transaction3) shouldBe true

    openedTransactions.add(transaction3, token2)
    openedTransactions.isValid(transaction1) shouldBe false
    openedTransactions.isValid(transaction2) shouldBe false
    openedTransactions.isValid(transaction3) shouldBe true
  }
}
