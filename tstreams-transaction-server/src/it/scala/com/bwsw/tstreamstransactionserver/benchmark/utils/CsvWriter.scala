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

import java.io.{File, PrintWriter}

trait CsvWriter {
  def writeMetadataTransactionsAndTime(filename: String, data: IndexedSeq[(Int, Long)]): Unit =
    write(filename, data, "Number of transaction, Time (ms)")

  def writeDataTransactionsAndTime(filename: String, data: IndexedSeq[(Int, Long)]): Unit =
    write(filename, data, "Number of transaction, Time (ms)")

  def writeTransactionsLifeCycleAndTime(filename: String, data: IndexedSeq[(Int, Long)]): Unit =
    write(filename, data, "Number of transaction life cycle, Time (ms)")


  private def write(filename: String, data: IndexedSeq[(Int, Long)], header: String) = {
    val file = new File(filename)
    val writer = new PrintWriter(file)
    writer.write(s"$header\n")
    data.foreach(x => writer.write(x._1 + ", " + x._2 + "\n"))
    writer.close()
  }
}
