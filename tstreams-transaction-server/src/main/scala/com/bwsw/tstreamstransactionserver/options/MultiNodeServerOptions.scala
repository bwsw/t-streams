
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

object MultiNodeServerOptions {

  /** The options are used to access/modifying zookeeper tree list, where ledgers for checkpoint group are stored.
    *
    * @param checkpointGroupZkTreeListPrefix the zookeeper path to TreeList for checkpoint group.
    * @param timeBetweenCreationOfLedgersMs  the delay between creating new ledgers.
    */
  final case class CheckpointGroupPrefixesOptions(checkpointGroupZkTreeListPrefix: String = "/tts/cg/master_tree",
                                                  checkpointGroupLastClosedLedger: String = "/tts/cg/last_closed_ledger",
                                                  timeBetweenCreationOfLedgersMs: Int = 200)

  /** The options are used to access/modifying zookeeper tree list, where ledgers for common group are stored.
    *
    * @param commonMasterZkTreeListPrefix   the zookeeper path to TreeList for common group.
    * @param timeBetweenCreationOfLedgersMs the delay between creating new ledgers.
    * @param checkpointGroupPrefixesOptions look at: [[MultiNodeServerOptions.CheckpointGroupPrefixesOptions]].
    */
  final case class CommonPrefixesOptions(commonMasterZkTreeListPrefix: String = "/tts/common/master_tree",
                                         commonMasterLastClosedLedger: String = "/tts/common/last_closed_ledger",
                                         timeBetweenCreationOfLedgersMs: Int = 200,
                                         checkpointGroupPrefixesOptions: CheckpointGroupPrefixesOptions = CheckpointGroupPrefixesOptions())

  /** The options are used to configure access to bookies servers, creating/reading new ledgers.
    *
    * @param ensembleNumber    the number of bookies the data in the ledger will be stored on.
    * @param writeQuorumNumber the number of bookies each entry is written to.
    * @param ackQuorumNumber   the number of bookies we must get a response from before we acknowledge the write to the client.
    * @param password          is used for accessing constructed ledgers and creating new ledgers.
    */
  final case class BookkeeperOptions(ensembleNumber: Int = 3,
                                     writeQuorumNumber: Int = 3,
                                     ackQuorumNumber: Int = 2,
                                     password: Array[Byte] = "ChangePassword".getBytes())

}
