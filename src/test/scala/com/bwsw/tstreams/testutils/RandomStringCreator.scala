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

package com.bwsw.tstreams.testutils

/**
  * Random string generator
  */
object RandomStringCreator {
  private val random = new scala.util.Random

  /**
    * Random string creator
    *
    * @param alphabet base string for creating random string
    * @param n        length of random string
    * @return random string
    */
  private def randomString(alphabet: String)(n: Int): String =
    Stream.continually(random.nextInt(alphabet.length)).map(alphabet).take(n).mkString

  /**
    * Wrapper for random string creation
    *
    * @param n length of string
    * @return random string
    */
  def randomAlphaString(n: Int) = randomString("abcdefghijklmnopqrstuvwxyz")(n)
}
