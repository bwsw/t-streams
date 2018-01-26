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

package com.bwsw.tstreamstransactionserver.benchmark.utils

import scala.io.Source

object Statistic {
  def mean(items: Traversable[Int]): Double = {
    items.sum.toDouble / items.size
  }

  def variance(items: Traversable[Int]): Double = {
    val itemMean = mean(items)
    val count = items.size
    val sumOfSquares = items.foldLeft(0.0d)((total, item) => {
      val square = math.pow(item - itemMean, 2)
      total + square
    })
    sumOfSquares / count.toDouble
  }

  def stddev(items: Traversable[Int]): Double = {
    math.sqrt(variance(items))
  }
}


object CI extends App {
  val lines = Source.fromFile("49_1000000TransactionLifeCycleWriterOSMC.csv").getLines
  val time = lines.drop(1).map(x => x.split(",")(1).trim.toInt).toTraversable
  val mean = Statistic.mean(time)
  val stddev = Statistic.stddev(time)

  println("Mean: " + mean)
  println("Confidence interval: (" + (mean - 1.960 * stddev) + ", " + (mean + 1.960 * stddev) + ")")
}