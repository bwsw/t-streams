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

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics

import scala.collection.mutable.ArrayBuffer

/**
  * Provides method to measure an execution time of some method
  *
  * @author Pavel Tomskikh
  */
class ExecutionTimeMeasurement {
  private val time = ArrayBuffer.empty[Long]

  /**
    * Measures an execution time of some method and put it in buffer
    *
    * @param method method under measurement
    * @tparam T return type of method under measurement
    * @return result of method under measurement
    */
  def apply[T](method: () => T): T = {
    val start = System.nanoTime()
    val result = method()
    val end = System.nanoTime()
    time += end - start

    result
  }

  /**
    * Returns an average execution time of some method (in nanoseconds)
    *
    * @return an average execution time of some method (in nanoseconds)
    */
  def average: Long = time.sum / time.length

  /**
    * Returns the smallest execution time of some method (in nanoseconds)
    *
    * @return the smallest execution time of some method (in nanoseconds)
    */
  def min: Long = time.min

  /**
    * Returns the largest execution time of some method (in nanoseconds)
    *
    * @return the largest execution time of some method (in nanoseconds)
    */
  def max: Long = time.max

  /**
    * Returns an 95% confidence interval of execution time of some method (in nanoseconds)
    *
    * @return an largest execution time of some method (in nanoseconds)
    */
  def confidenceInterval: (Long, Long) = {
    val statistics = new DescriptiveStatistics(time.map(_.toDouble).toArray)
    val mean = statistics.getMean
    val interval = 1.96 * (statistics.getStandardDeviation / Math.sqrt(statistics.getN))

    ((mean - interval).round, (mean + interval).round)
  }

  def result: ExecutionTimeMeasurement.Result = {
    ExecutionTimeMeasurement.Result(
      average = average,
      min = min,
      max = max,
      confidenceInterval = confidenceInterval)
  }
}

object ExecutionTimeMeasurement {

  case class Result(average: Long,
                    min: Long,
                    max: Long,
                    confidenceInterval: (Long, Long)) {
    def toSeq: Seq[Long] =
      Seq(average, min, max, confidenceInterval._1, confidenceInterval._2)
  }

  object Result {
    def fields: Seq[String] =
      Seq("average", "min", "max", "confidenceIntervalLow", "confidenceIntervalHigh")
  }

}
