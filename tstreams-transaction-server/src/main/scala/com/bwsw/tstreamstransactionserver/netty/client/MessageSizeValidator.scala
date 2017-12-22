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
