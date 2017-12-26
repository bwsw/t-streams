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

package util

import com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction


object Implicit {

  implicit object ProducerTransactionSortable extends Ordering[ProducerTransaction] {
    override def compare(x: ProducerTransaction, y: ProducerTransaction): Int = {
      if (x.stream > y.stream) 1
      else if (x.stream < y.stream) -1
      else if (x.partition > y.partition) 1
      else if (x.partition < y.partition) -1
      else if (x.transactionID > y.transactionID) 1
      else if (x.transactionID < y.transactionID) -1
      else if (x.state.value < y.state.value) -1
      else if (x.state.value > y.state.value) 1
      else 0
    }
  }

}