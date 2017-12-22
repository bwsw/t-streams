package com.bwsw.tstreamstransactionserver.netty.server.transportService

import com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException
import com.bwsw.tstreamstransactionserver.netty.RequestMessage
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.TransportOptions

final class TransportValidator(packageTransmissionOpts: TransportOptions) {
  lazy val packageTooBigException = new PackageTooBigException(s"A size of client request is greater " +
    s"than maxMetadataPackageSize (${packageTransmissionOpts.maxMetadataPackageSize}) " +
    s"or maxDataPackageSize (${packageTransmissionOpts.maxDataPackageSize}).")

  val maxMetadataPackageSize: Int =
    packageTransmissionOpts.maxMetadataPackageSize

  val maxDataPackageSize: Int =
    packageTransmissionOpts.maxDataPackageSize

  def isTooBigMetadataMessage(message: RequestMessage): Boolean = {
    message.body.length > maxMetadataPackageSize
  }

  def isTooBigDataMessage(message: RequestMessage): Boolean = {
    message.body.length > maxDataPackageSize
  }
}
