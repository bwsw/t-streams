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
  * Performs [[ProducerBenchmark]].
  *
  * @author Pavel Tomskikh
  */
trait BenchmarkRunner {

  def main(args: Array[String]): Unit = {
    val config = new BenchmarkConfig(args)
    val benchmark = new Benchmark(
      address = config.address(),
      token = config.token(),
      prefix = config.prefix(),
      stream = config.stream(),
      partitions = config.partitions())

    val result = runBenchmark(benchmark, config)
    benchmark.close()

    println(("method" +: ExecutionTimeMeasurement.Result.fields).mkString(", "))
    println(
      result.toMap
        .mapValues(_.toSeq.mkString(", "))
        .map { case (k, v) => s"$k, $v" }
        .mkString("\n"))

    System.exit(0)
  }

  def runBenchmark(benchmark: Benchmark, config: BenchmarkConfig): Benchmark.Result
}
