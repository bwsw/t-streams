package com.bwsw.tstreams.coordination.server

import java.net.{DatagramPacket, SocketAddress}

import com.bwsw.tstreams.agents.consumer.subscriber.{Subscriber, TransactionBufferWorker}
import com.bwsw.tstreams.common.UdpServer
import com.bwsw.tstreams.proto.protocol.TransactionState

import scala.collection.mutable

/**
  * Created by Ivan Kudryavtsev on 21.04.17.
  */
class StateUpdateServer(host: String, port: Int, threads: Int,
                        transactionsBufferWorkers: mutable.Map[Int, TransactionBufferWorker],
                        authenticationKey: String) extends UdpServer(host, port, threads) {

  private val partitionCache = mutable.Map[Int, TransactionBufferWorker]()

  transactionsBufferWorkers
    .foreach(id_w => id_w._2.getPartitions().foreach(p => partitionCache(p) = id_w._2))

  override def handleRequest(client: SocketAddress, reqAny: AnyRef): Unit = {
    val req = reqAny.asInstanceOf[TransactionState]

    if(Subscriber.logger.isDebugEnabled())
      Subscriber.logger.debug(s"Transaction State Update: ${req}")

    if(req.authKey == authenticationKey) {
      if (partitionCache.contains(req.partition))
        partitionCache(req.partition).update(req)
      else
        Subscriber.logger.warn(s"Unknown partition ${req.partition} found in Message: $req.")
    } else {
      Subscriber.logger.warn(s"Update req ($req) was received, but expected is $authenticationKey, message has been ignore.")
    }
  }

  override def getObjectFromDatagramPacket(packet: DatagramPacket): Option[AnyRef] =
      Some(TransactionState.parseFrom(packet.getData.take(packet.getLength)))

  override def getKey(objAny: AnyRef): Int = objAny.asInstanceOf[TransactionState].partition
}
