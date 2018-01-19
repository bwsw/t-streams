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
import com.google.common.cache.{CacheBuilder, RemovalNotification}
import org.slf4j.{Logger, LoggerFactory}

import scala.annotation.tailrec
import scala.util.Random

final class AuthService(authOpts: AuthenticationOptions, tokenWriter: TokenWriter) {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  private val tokensCache = CacheBuilder.newBuilder()
    .expireAfterWrite(
      authOpts.keyCacheExpirationTimeSec,
      java.util.concurrent.TimeUnit.SECONDS)
    .removalListener((notification: RemovalNotification[Integer, String]) => {
      if (notification.wasEvicted()) {
        invalidate(notification.getKey)
      }
    })
    .build[Integer, String]()


  /** Authenticates client and creates new token if authKey is valid
    *
    * @param authKey authentication key
    * @return Some(token) if authentication key is valid or None otherwise
    */
  private[server] def authenticate(authKey: String): Option[Int] = {
    if (authKey == authOpts.key) {
      @tailrec
      def generateToken(): Int = {
        Random.nextInt() match {
          case AuthService.UnauthenticatedToken => generateToken()
          case generated => generated
        }
      }

      val token = generateToken()
      tokensCache.put(token, "")
      tokenWriter.tokenCreated(token)

      if (logger.isDebugEnabled)
        logger.debug(s"Client with authkey $authKey is successfully authenticated and assigned token $token.")

      Some(token)
    } else {
      if (logger.isDebugEnabled)
        logger.debug(s"Client with authkey $authKey isn't authenticated.")

      None
    }
  }

  /** Validates client's token
    *
    * @param token client's token
    * @return true if client's token is valid or false otherwise
    */
  private[server] def isValid(token: Int): Boolean = {
    val tokenValid = Option(tokensCache.getIfPresent(token)).isDefined

    if (logger.isDebugEnabled) {
      if (tokenValid)
        logger.debug(s"Client token $token is accepted.")
      else
        logger.debug(s"Client token $token is expired or doesn't exist.")
    }

    tokenValid
  }

  /** Updates client's token
    *
    * @param token client's token
    * @return true if client's token is valid or false otherwise
    */
  private[server] def update(token: Int): Boolean = {
    val tokenValid = isValid(token)

    if (tokenValid) {
      if (logger.isDebugEnabled) {
        logger.debug(s"Update client token $token.")
      }

      tokensCache.put(token, "")
      tokenWriter.tokenUpdated(token)
    }

    tokenValid
  }


  /** Invokes when client's token removed from cache
    *
    * @param token client's token
    */
  private def invalidate(token: Int): Unit = {
    if (logger.isDebugEnabled) {
      logger.debug(s"Client token is expired $token.")
    }

    tokenWriter.tokenExpired(token)
  }
}


object AuthService {
  // reserved for unauthenticated requests
  val UnauthenticatedToken: Int = -1
}