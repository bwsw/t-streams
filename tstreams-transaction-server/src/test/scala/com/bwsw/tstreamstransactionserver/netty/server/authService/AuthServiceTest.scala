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
import org.mockito.ArgumentMatchers.anyInt
import org.mockito.Mockito.{never, reset, verify}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

/** Tests for [[AuthService]]
  *
  * @author Pavel Tomskikh
  */
class AuthServiceTest extends FlatSpec with Matchers with MockitoSugar with BeforeAndAfterEach {

  private val ttlSec = 1
  private val ttlMs = ttlSec * 1000
  private val waitingInterval = ttlMs / 2 + 100
  private val tokenWriter = mock[TokenWriter]
  private val authenticationOptions = AuthenticationOptions(
    key = "valid-key",
    tokenTtlSec = ttlSec)

  override protected def beforeEach(): Unit = reset(tokenWriter)


  "AuthService" should "authenticate a client if key is correct" in {
    val authService = new AuthService(authenticationOptions, tokenWriter)

    val maybeToken = authService.authenticate(authenticationOptions.key)
    maybeToken shouldBe defined
    verify(tokenWriter).tokenCreated(maybeToken.get)
  }

  it should "not authenticate a client if key is correct" in {
    val authService = new AuthService(authenticationOptions, tokenWriter)

    authService.authenticate("wrong-key") shouldBe empty
    verify(tokenWriter, never()).tokenCreated(anyInt())
  }


  it should "validate token if it exists in a cache" in {
    val authService = new AuthService(authenticationOptions, tokenWriter)
    val token = authService.authenticate(authenticationOptions.key).get

    authService.isValid(token) shouldBe true
  }

  it should "not validate token if it doesn't exists in a cache" in {
    val authService = new AuthService(authenticationOptions, tokenWriter)
    val token = authService.authenticate(authenticationOptions.key).get
    val wrongToken = token + 1

    authService.isValid(wrongToken) shouldBe false
  }


  it should "update token if it exists in a cache" in {
    val authService = new AuthService(authenticationOptions, tokenWriter)
    val token = authService.authenticate(authenticationOptions.key).get

    authService.update(token) shouldBe true
    verify(tokenWriter).tokenUpdated(token)
  }

  it should "not update token if it doesn't exists in a cache" in {
    val authService = new AuthService(authenticationOptions, tokenWriter)
    val token = authService.authenticate(authenticationOptions.key).get
    val wrongToken = token + 1

    authService.update(wrongToken) shouldBe false
    verify(tokenWriter, never()).tokenUpdated(anyInt())
  }

  it should "not validate expired token" in {
    val authService = new AuthService(authenticationOptions, tokenWriter)
    val token = authService.authenticate(authenticationOptions.key).get

    authService.isValid(token) shouldBe true

    Thread.sleep(ttlMs)

    authService.isValid(token) shouldBe false
    // https://github.com/google/guava/issues/2110
    //verify(tokenWriter).tokenExpired(token)
  }

  it should "update token's TTL by update(token) method" in {
    val authService = new AuthService(authenticationOptions, tokenWriter)
    val token = authService.authenticate(authenticationOptions.key).get

    authService.isValid(token) shouldBe true

    Thread.sleep(waitingInterval)

    authService.update(token) shouldBe true

    Thread.sleep(waitingInterval)

    authService.isValid(token) shouldBe true
  }

  it should "not update token's TTL by isValid(token) method" in {
    val authService = new AuthService(authenticationOptions, tokenWriter)
    val token = authService.authenticate(authenticationOptions.key).get

    authService.isValid(token) shouldBe true

    Thread.sleep(waitingInterval)

    authService.isValid(token) shouldBe true

    Thread.sleep(waitingInterval)

    authService.isValid(token) shouldBe false
    // https://github.com/google/guava/issues/2110
    //verify(tokenWriter).tokenExpired(token)
  }
}
