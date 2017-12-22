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

package com.bwsw.tstreams.agents.consumer

import java.util.Date

/**
  * All possible start offsets for consumer
  */
object Offset {

  /**
    * Basic trait for all offsets
    */
  trait IOffset

  /**
    * Oldest offset for data retrieving from the very beginning
    */
  case object Oldest extends IOffset

  /**
    * Newest offset for data retrieving from now
    */
  case object Newest extends IOffset

  /**
    * Offset for data retrieving from custom Date
    *
    * @param startTime Start offset value
    */
  case class DateTime(startTime: Date) extends IOffset

  /**
    * Offset for data retrieving from custom ID
    *
    * @param startID Start offset in id
    */
  case class ID(startID: Long) extends IOffset

}

