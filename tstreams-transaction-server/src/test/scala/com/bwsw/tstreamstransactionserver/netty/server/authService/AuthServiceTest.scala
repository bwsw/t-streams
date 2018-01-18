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

import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.AuthenticationOptions
import org.scalatest.{FlatSpec, Matchers}

/** Tests for [[AuthService]]
  *
  * @author Pavel Tomskikh
  */
class AuthServiceTest extends FlatSpec with Matchers {

  private val ttlSec = 1
  private val ttlMs = ttlSec * 1000
  private val waitingInterval = ttlMs / 2 + 100
  private val authenticationOptions = AuthenticationOptions(
    key = "valid-key",
    keyCacheSize = 3,
    keyCacheExpirationTimeSec = ttlSec)


  "AuthService" should "authenticate a client if key is correct" in {
    val authService = new AuthService(authenticationOptions)

    authService.authenticate(authenticationOptions.key) shouldBe defined
  }

  it should "not authenticate a client if key is correct" in {
    val authService = new AuthService(authenticationOptions)

    authService.authenticate("wrong-key") shouldBe empty
  }


  it should "validate token if it exists in a cache" in {
    val authService = new AuthService(authenticationOptions)
    val token = authService.authenticate(authenticationOptions.key).get

    authService.isValid(token) shouldBe true
  }

  it should "not validate token if it doesn't exists in a cache" in {
    val authService = new AuthService(authenticationOptions)
    val token = authService.authenticate(authenticationOptions.key).get
    val wrongToken = token + 1

    authService.isValid(wrongToken) shouldBe false
  }


  it should "update token if it exists in a cache" in {
    val authService = new AuthService(authenticationOptions)
    val token = authService.authenticate(authenticationOptions.key).get

    authService.update(token) shouldBe true
  }

  it should "not update token if it doesn't exists in a cache" in {
    val authService = new AuthService(authenticationOptions)
    val token = authService.authenticate(authenticationOptions.key).get
    val wrongToken = token + 1

    authService.update(wrongToken) shouldBe false
  }

  it should "not validate expired token" in {
    val authService = new AuthService(authenticationOptions)
    val token = authService.authenticate(authenticationOptions.key).get

    authService.isValid(token) shouldBe true

    Thread.sleep(ttlMs)

    authService.isValid(token) shouldBe false
  }

  it should "update token's TTL by update(token) method" in {
    val authService = new AuthService(authenticationOptions)
    val token = authService.authenticate(authenticationOptions.key).get

    authService.isValid(token) shouldBe true

    Thread.sleep(waitingInterval)

    authService.update(token) shouldBe true

    Thread.sleep(waitingInterval)

    authService.isValid(token) shouldBe true
  }

  it should "not update token's TTL by isValid(token) method" in {
    val authService = new AuthService(authenticationOptions)
    val token = authService.authenticate(authenticationOptions.key).get

    authService.isValid(token) shouldBe true

    Thread.sleep(waitingInterval)

    authService.isValid(token) shouldBe true

    Thread.sleep(waitingInterval)

    authService.isValid(token) shouldBe false
  }

  it should "expire LRU token if cache is full" in {
    val authService = new AuthService(authenticationOptions.copy(keyCacheExpirationTimeSec = 60))
    val firstToken = authService.authenticate(authenticationOptions.key).get

    for (_ <- 1 to authenticationOptions.keyCacheSize) {
      authService.authenticate(authenticationOptions.key) shouldBe defined
    }

    authService.isValid(firstToken) shouldBe false
  }
}
