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

package ut.multiNodeServer.ZkTreeListTest

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.metadata.{LedgerMetadata, MetadataRecord, NoRecordReadStatus}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class ZkMultipleTreeListReaderMetadataTest
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll {

  "Metadata record" should "contain timestamp without records" in {
    val metadataRecord = new MetadataRecord(
      Array.empty[LedgerMetadata]
    )

    MetadataRecord.fromByteArray(metadataRecord.toByteArray) shouldBe metadataRecord
  }

  it should "contain timestamp with records" in {
    val recordNumber = 10

    val rand = scala.util.Random
    val records = Array.fill(recordNumber)(
      LedgerMetadata(rand.nextLong(), rand.nextLong(), NoRecordReadStatus)
    )

    val metadataRecord = new MetadataRecord(
      records
    )

    MetadataRecord.fromByteArray(metadataRecord.toByteArray) shouldBe metadataRecord
  }

}
