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

package com.bwsw.tstreams.benchmark

/**
  * Provides methods to print progress to the console
  *
  * @param max    100% progress
  * @param prefix output string prefix
  * @author Pavel Tomskikh
  */
class ProgressBar(max: Int, prefix: String = "progress") {

  /**
    * Prints progress to the console
    *
    * @param progress current progress
    */
  def show(progress: Int): Unit =
    show(progress, i => s"\rprogress: $i/$max (${i * 100 / max}%)")

  /**
    * Prints progress to the console at rate
    *
    * @param progress current progress
    * @param rate     progress will be printed to the console after this number of iterations
    */
  def show(progress: Int, rate: Int): Unit = {
    if (progress % rate == 0 || progress == max)
      show(progress)
  }


  private def show(progress: Int, progressToString: Int => String) = {
    print(progressToString(progress))
    if (progress == max) println()
  }
}
