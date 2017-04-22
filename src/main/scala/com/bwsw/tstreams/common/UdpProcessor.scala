package com.bwsw.tstreams.common

import java.net.{DatagramPacket, DatagramSocket, SocketException}
import java.util.concurrent.CountDownLatch

import org.slf4j.LoggerFactory

object UdpProcessor {
  var BUFFER_SIZE = 508
}

abstract class UdpProcessor {
  val socket = socketInitializer()

  val logger = LoggerFactory.getLogger(this.getClass)

  def socketInitializer() = new DatagramSocket()

  def bind(s: DatagramSocket)

  def bootstrapOperation() = { bind(socket) }

  def handleMessage(socket: DatagramSocket, packet: DatagramPacket): Unit

  def operationLoop() = {
    try {
      while (true) {
        val packet = new DatagramPacket(new Array[Byte](UdpProcessor.BUFFER_SIZE), UdpProcessor.BUFFER_SIZE)
        socket.receive(packet)
        handleMessage(socket, packet)
      }
    } catch {
      case ex: SocketException => if(!socket.isClosed) throw ex
    }
  }

  val startLatch = new CountDownLatch(1)
  val ioBlockingThread = new Thread(() => {
    try {
      startLatch.countDown()
      operationLoop()
    } catch {
      case ex: InterruptedException =>
        logger.info("IO Thread was interrupted.")
      case ex: Exception => throw ex
    }
  })

  def start() = {
    bootstrapOperation()
    ioBlockingThread.start()
    startLatch.await()
    this
  }

  def stop() = {
    socket.close()
    ioBlockingThread.join()
  }
}
