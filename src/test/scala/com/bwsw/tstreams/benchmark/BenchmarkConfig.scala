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

import org.rogach.scallop.{ScallopConf, ScallopOption}

/**
  * Retrieves configuration from command line arguments
  *
  * @param arguments command line arguments
  * @author Pavel Tomskikh
  */
class BenchmarkConfig(arguments: Seq[String]) extends ScallopConf(arguments) {

  import BenchmarkConfig.Default

  val address: ScallopOption[String] = opt[String](
    required = true,
    short = 'a',
    descr = "ZooKeeper address")

  val token: ScallopOption[String] = opt[String](
    required = true,
    short = 't',
    descr = "authentication token")

  val prefix: ScallopOption[String] = opt[String](
    required = true,
    short = 'p',
    descr = "path to master node in ZooKeeper")

  val stream: ScallopOption[String] = opt[String](
    default = Some(Default.stream),
    descr = s"stream name (${Default.stream} by default)")

  val partitions: ScallopOption[Int] = opt[Int](
    default = Some(Default.partitions),
    descr = s"amount of stream partitions (${Default.partitions} by default)")

  val iterations: ScallopOption[Int] = opt[Int](
    default = Some(Default.iterations),
    descr = s"amount of measurements (${Default.iterations} by default)")

  val dataSize: ScallopOption[Int] = opt[Int](
    default = Some(Default.dataSize),
    descr = s"size of data sent to each transaction (${Default.dataSize} by default)")

  val cancel: ScallopOption[Boolean] = opt[Boolean](
    descr = "if set, each transaction will be cancelled, otherwise a checkpoint will be performed for each transaction")


  val partition: ScallopOption[Int] = opt[Int](
    default = Some(Default.partition),
    descr = s"stream partition (${Default.partition} by default)")

  val loadData: ScallopOption[Boolean] = opt[Boolean](
    descr = "if set, data will be retrieved from transactions")


  verify()
}

object BenchmarkConfig {

  object Default {
    val stream = "test"
    val partitions = 1
    val iterations = 100000
    val dataSize = 100
    val partition = 0
  }

}
