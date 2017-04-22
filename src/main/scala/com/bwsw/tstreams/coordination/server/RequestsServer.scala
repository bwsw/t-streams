package com.bwsw.tstreams.coordination.server

import java.net.DatagramPacket

import com.bwsw.tstreams.common.UdpServer
import com.bwsw.tstreams.proto.protocol.TransactionRequest

/**
  * Created by Ivan Kudryavtsev on 21.04.17.
  */
abstract class RequestsServer(host: String, port: Int, threads: Int) extends UdpServer(host, port, threads)  {

  override def getObjectFromDatagramPacket(packet: DatagramPacket): Option[AnyRef] =
    Some(TransactionRequest.parseFrom(packet.getData.take(packet.getLength)))

  override def getKey(objAny: AnyRef): Int = objAny.asInstanceOf[TransactionRequest].partition
}
