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

import com.bwsw.tstreamstransactionserver.netty.server.batch.Frame

/** Writes token events into some log
  *
  * @author Pavel Tomskikh
  */
trait TokenWriter {

  /** Writes that token created
    *
    * @param token client's token
    */
  def tokenCreated(token: Int): Unit =
    write(Frame.TokenCreatedType, token)

  /** Writes that token updated
    *
    * @param token client's token
    */
  def tokenUpdated(token: Int): Unit =
    write(Frame.TokenUpdatedType, token)

  /** Writes that token expired
    *
    * @param token client's token
    */
  def tokenExpired(token: Int): Unit =
    write(Frame.TokenExpiredType, token)

  /** Writes token event into log
    *
    * @param eventType event type (can be [[Frame.TokenCreatedType]], [[Frame.TokenUpdatedType]]
    *                  or [[Frame.TokenExpiredType]])
    * @param token     client's token
    */
  protected def write(eventType: Byte, token: Int): Unit
}
