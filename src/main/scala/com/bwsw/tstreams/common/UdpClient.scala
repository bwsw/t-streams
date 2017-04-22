package com.bwsw.tstreams.common

import java.net.{DatagramPacket, DatagramSocket, InetAddress, SocketException}
import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap, Executors, TimeUnit}

import com.bwsw.tstreams.proto.protocol.{TransactionRequest, TransactionResponse}
import com.google.protobuf.InvalidProtocolBufferException

import scala.util.Random

class UdpClient(waitTimeoutMs: Int) extends UdpProcessor {
  val packetMap = new ConcurrentHashMap[Long, ArrayBlockingQueue[TransactionResponse]]()
  val executor = Executors.newSingleThreadExecutor()


  override def handleMessage(socket: DatagramSocket, packet: DatagramPacket): Unit = {
    executor.execute(() => {
      val rOpt = try {
        Some(TransactionResponse.parseFrom(packet.getData.take(packet.getLength)))
      } catch {
        case ex: InvalidProtocolBufferException => None
      }
      rOpt.foreach(response =>
        if(response.transaction >= 0) {
          val qOpt = Option(packetMap.getOrDefault(response.id, null))
          qOpt.foreach(q => q.put(response))
        })
    })
  }
  
  def sendAndWait(hostName: String, port: Int, req: TransactionRequest): Option[TransactionResponse] = {
    val msg = req.withId(Random.nextLong)
    val q = new ArrayBlockingQueue[TransactionResponse](1)
    packetMap.put(msg.id, q)
    val host = InetAddress.getByName(hostName)
    val arr = msg.toByteArray
    if(arr.length > UdpProcessor.BUFFER_SIZE)
      throw new IllegalArgumentException(s"TransactionRequest serialized size must be less than or equal to ${UdpProcessor.BUFFER_SIZE} to " +
        "surpass currend UDP limitations. Increase UdpProcessor.BUFFER_SIZE parameter " +
        "if your network supports jumbo frames.")
    val sendPacket = new java.net.DatagramPacket(arr, arr.length, host, port)
    var isSent = false
    while (!isSent) {
      try {
        socket.send(sendPacket)
        isSent = true
      } catch {
        case e: SocketException => socket.isClosed match {
          case true => throw e
          case false =>
        }
        case e: Exception =>
          logger.warn(s"Send $msg to $host:$port failed. Exception is: $e")
      }
    }
    val mOpt = Option(q.poll(waitTimeoutMs, TimeUnit.MILLISECONDS))
    packetMap.remove(msg.id)
    mOpt
  }

  override def bind(s: DatagramSocket): Unit = {}

  override def start() = super.start().asInstanceOf[UdpClient]
}

