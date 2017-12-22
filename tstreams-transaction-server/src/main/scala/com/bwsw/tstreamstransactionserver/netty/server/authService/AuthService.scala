
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
import com.google.common.cache.CacheBuilder
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Random

final class AuthService(authOpts: AuthenticationOptions) {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val usersToken = CacheBuilder.newBuilder()
    .maximumSize(authOpts.keyCacheSize)
    .expireAfterAccess(
      authOpts.keyCacheExpirationTimeSec,
      java.util.concurrent.TimeUnit.SECONDS
    )
    .build[java.lang.Integer, String]()

  private[server] def authenticate(authKey: String): Int = {
    if (authKey == authOpts.key) {
      val token = Random.nextInt(Integer.MAX_VALUE)
      usersToken.put(token, authKey)
      if (logger.isDebugEnabled())
        logger.debug(s"Client with authkey $authKey is successfully authenticated and assigned token $token.")
      token
    } else {
      if (logger.isDebugEnabled())
        logger.debug(s"Client with authkey $authKey isn't authenticated and assigned token -1.")
      -1
    }
  }

  private[server] def isValid(token: Int): Boolean = {
    val isValid = token != -1 &&
      usersToken.getIfPresent(token) != null

    if (isValid)
      if (logger.isDebugEnabled()) {
        logger.debug(s"Client token $token is accepted.")
      }
    else
      if (logger.isDebugEnabled()) {
        logger.debug(s"Client token $token is expired or doesn't exist.")
      }

    isValid
  }
}