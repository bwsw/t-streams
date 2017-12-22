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
package com.bwsw.commitlog

/** Policies to work with commitlog. */
object CommitLogFlushPolicy {

  /** Basic trait for all policies */
  trait ICommitLogFlushPolicy

  /** Data is flushed into file when specified count of seconds from last flush operation passed. */
  case class OnTimeInterval(seconds: Integer) extends ICommitLogFlushPolicy {
    require(seconds > 0, "Interval of seconds must be greater that 0.")
  }

  /** Data is flushed into file when specified count of write operations passed. */
  case class OnCountInterval(count: Integer) extends ICommitLogFlushPolicy {
    require(count > 0, "Interval of writes must be greater that 0.")
  }

  /** Data is flushed into file when new file starts. */
  case object OnRotation extends ICommitLogFlushPolicy

}
