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
  * Performs [[ProducerBenchmark]].
  *
  * Arguments:
  * -a, --address - ZooKeeper address;
  * -t, --token - authentication token;
  * -p, --prefix - path to master node in ZooKeeper;
  * --stream - stream name (test by default);
  * --partitions - amount of partitions on stream (1 by default);
  * --iterations - amount of measurements (100000 by default);
  * --data-size - size of data sent in each transaction (100 by default).
  *
  * @author Pavel Tomskikh
  */
object ProducerBenchmarkRunner extends App {

  private val config = new Config(args)
  private val benchmark = new ProducerBenchmark(
    address = config.address(),
    token = config.token(),
    prefix = config.prefix(),
    stream = config.stream(),
    partitions = config.partitions())

  private val result = benchmark.run(config.iterations(), dataSize = config.dataSize())
  benchmark.close()

  println(("method" +: ExecutionTimeMeasurement.Result.fields).mkString(", "))
  println(
    result.toMap
      .mapValues(_.toSeq.mkString(", "))
      .map { case (k, v) => s"$k, $v" }
      .mkString("\n"))

  System.exit(0)


  class Config(arguments: Seq[String]) extends ScallopConf(arguments) {
    val address: ScallopOption[String] = opt[String](required = true, short = 'a')
    val token: ScallopOption[String] = opt[String](required = true, short = 't')
    val prefix: ScallopOption[String] = opt[String](required = true, short = 'p')
    val stream: ScallopOption[String] = opt[String](default = Some("test"))
    val partitions: ScallopOption[Int] = opt[Int](default = Some(1))
    val iterations: ScallopOption[Int] = opt[Int](default = Some(10000))
    val dataSize: ScallopOption[Int] = opt[Int](default = Some(100))
    verify()
  }

}



