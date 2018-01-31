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

import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random

/** Tests for [[TokenCache]]
  *
  * @author Pavel Tomskikh
  */
class TokenCacheTests extends FlatSpec with Matchers with MockitoSugar {

  private val tokenTtlSec = 10
  private val tokenTtlMs = tokenTtlSec * 1000

  "TokenCache" should "validates tokens properly" in {
    val tokenCache = new TokenCache(tokenTtlSec)
    val token1 = Random.nextInt()
    val token2 = Random.nextInt()
    val token3 = Random.nextInt()

    tokenCache.isValid(token1) shouldBe false
    tokenCache.isValid(token2) shouldBe false
    tokenCache.isValid(token3) shouldBe false

    tokenCache.add(token1, System.currentTimeMillis())
    tokenCache.isValid(token1) shouldBe true
    tokenCache.isValid(token2) shouldBe false
    tokenCache.isValid(token3) shouldBe false

    tokenCache.add(token2, System.currentTimeMillis())
    tokenCache.isValid(token1) shouldBe true
    tokenCache.isValid(token2) shouldBe true
    tokenCache.isValid(token3) shouldBe false

    tokenCache.remove(token1)
    tokenCache.isValid(token1) shouldBe false
    tokenCache.isValid(token2) shouldBe true
    tokenCache.isValid(token3) shouldBe false
  }

  it should "remove expired tokens properly" in {
    val tokenCache = new TokenCache(tokenTtlSec)
    val time = System.currentTimeMillis()
    val timeShift = 100

    val (token1, time1) = (Random.nextInt(), time)
    val (token2, time2) = (Random.nextInt(), time + timeShift)
    val (token3, time3) = (Random.nextInt(), time + timeShift * 2)

    tokenCache.add(token1, time1)
    tokenCache.add(token2, time2)
    tokenCache.add(token3, time3)

    tokenCache.isValid(token1) shouldBe true
    tokenCache.isValid(token2) shouldBe true
    tokenCache.isValid(token3) shouldBe true

    tokenCache.removeExpired(time + tokenTtlMs - 1)

    tokenCache.isValid(token1) shouldBe true
    tokenCache.isValid(token2) shouldBe true
    tokenCache.isValid(token3) shouldBe true

    tokenCache.removeExpired(time + tokenTtlMs)

    tokenCache.isValid(token1) shouldBe false
    tokenCache.isValid(token2) shouldBe true
    tokenCache.isValid(token3) shouldBe true

    tokenCache.removeExpired(time + tokenTtlMs + timeShift * 2 - 1)

    tokenCache.isValid(token1) shouldBe false
    tokenCache.isValid(token2) shouldBe false
    tokenCache.isValid(token3) shouldBe true

    tokenCache.removeExpired(time + tokenTtlMs + timeShift * 2)

    tokenCache.isValid(token1) shouldBe false
    tokenCache.isValid(token2) shouldBe false
    tokenCache.isValid(token3) shouldBe false
  }

}
