
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

package com.bwsw.tstreamstransactionserver.options

object CommitLogWriteSyncPolicy extends Enumeration {
  type CommitLogWriteSyncPolicy = Value
  val EveryNSeconds = Value("every-n-seconds")
  val EveryNth = Value("every-nth")
  val EveryNewFile = Value("every-new-file")
}

object IncompleteCommitLogReadPolicy extends Enumeration {
  type IncompleteCommitLogReadPolicy = Value
  val SkipLog = Value("skip-log")
  val TryRead = Value("try-read")
  val Error = Value("error")
}