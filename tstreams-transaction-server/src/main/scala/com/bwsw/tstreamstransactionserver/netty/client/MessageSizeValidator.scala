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

package com.bwsw.tstreamstransactionserver.netty.client

import com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException
import com.bwsw.tstreamstransactionserver.netty.{Protocol, RequestMessage}

import scala.collection.Searching.{Found, search}

private object MessageSizeValidator {

  val notValidateMessageProtocolIds: Array[Byte] =
    Array(
      Protocol.GetMaxPackagesSizes.methodID,
      Protocol.Authenticate.methodID,
      Protocol.IsValid.methodID
    ).sorted

  val metadataMessageProtocolIds: Array[Byte] =
    Array(
      Protocol.GetCommitLogOffsets.methodID,
      Protocol.GetLastCheckpointedTransaction.methodID,
      Protocol.GetTransaction.methodID,
      Protocol.GetTransactionID.methodID,
      Protocol.GetTransactionIDByTimestamp.methodID,
      Protocol.OpenTransaction.methodID,
      Protocol.PutTransaction.methodID,
      Protocol.PutTransactions.methodID,
      Protocol.ScanTransactions.methodID,

      Protocol.PutConsumerCheckpoint.methodID,
      Protocol.GetConsumerState.methodID
    ).sorted

  val dataMessageProtocolIds: Array[Byte] =
    Array(
      Protocol.GetTransactionData.methodID,
      Protocol.PutProducerStateWithData.methodID,
      Protocol.PutSimpleTransactionAndData.methodID,
      Protocol.PutTransactionData.methodID
    ).sorted
}

final class MessageSizeValidator(maxMetadataPackageSize: Int,
                                 maxDataPackageSize: Int) {

  def validateMessageSize(message: RequestMessage): Unit = {
    notValidateSomeMessageTypesSize(message)
  }

  private def notValidateSomeMessageTypesSize(message: RequestMessage) = {
    if (MessageSizeValidator.notValidateMessageProtocolIds
      .search(message.methodId).isInstanceOf[Found]) {
      //do nothing
    }
    else {
      validateMetadataMessageSize(message)
    }
  }

  @throws[PackageTooBigException]
  private def validateMetadataMessageSize(message: RequestMessage) = {
    if (MessageSizeValidator.metadataMessageProtocolIds
      .search(message.methodId).isInstanceOf[Found]) {
      if (message.bodyLength > maxMetadataPackageSize) {
        throw new PackageTooBigException(s"Client shouldn't transmit amount of data which is greater " +
          s"than maxMetadataPackageSize ($maxMetadataPackageSize).")
      }
    }
    else {
      validateDataMessageSize(message)
    }

  }

  @throws[PackageTooBigException]
  private def validateDataMessageSize(message: RequestMessage) = {
    if (MessageSizeValidator.dataMessageProtocolIds
      .search(message.methodId).isInstanceOf[Found]) {
      if (message.bodyLength > maxDataPackageSize) {
        throw new PackageTooBigException(s"Client shouldn't transmit amount of data which is greater " +
          s"than maxDataPackageSize ($maxDataPackageSize).")
      }
    }
    else {
      //do nothing
    }
  }
}
