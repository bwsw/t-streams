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
  * @author Pavel Tomskikh
  */
object ProducerBenchmark {

  case class Result(newTransaction: ExecutionTimeMeasurement.Result,
                    sendData: Option[ExecutionTimeMeasurement.Result] = None,
                    checkpoint: Option[ExecutionTimeMeasurement.Result] = None,
                    cancel: Option[ExecutionTimeMeasurement.Result] = None)
    extends Benchmark.Result {

    override def toMap: Map[String, ExecutionTimeMeasurement.Result] = {
      Map("newTransaction" -> newTransaction) ++
        sendData.map(result => "sendData" -> result) ++
        checkpoint.map(result => "checkpoint" -> result) ++
        cancel.map(result => "cancel" -> result)
    }
  }

}
