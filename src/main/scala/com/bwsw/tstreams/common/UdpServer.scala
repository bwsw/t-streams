package com.bwsw.tstreams.common

import java.net._
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Executors}

import com.google.protobuf.InvalidProtocolBufferException

/**
  * Created by Ivan Kudryavtsev on 20.04.17.
  */
abstract class UdpServer(host: String, port: Int, threads: Int) extends UdpProcessor {
  protected val executors = new Array[ExecutorService](threads)
  protected val partitionExecutorMapping = new ConcurrentHashMap[Int, Int]()
  private val partitionCounter = new AtomicInteger(0)

  protected def assignPartitionExecutor(partition: Int): Int = partitionCounter.getAndIncrement() % threads

  (0 until executors.size).foreach(idx => executors(idx) = Executors.newSingleThreadExecutor())

  override def socketInitializer() = new DatagramSocket(null)

  override def bind(s: DatagramSocket): Unit = {
    socket.bind(new InetSocketAddress(InetAddress.getByName(host), port))
  }

  def handleRequest(client: SocketAddress, req: AnyRef)

  def getObjectFromDatagramPacket(packet: DatagramPacket): Option[AnyRef]

  def getKey(objAny: AnyRef): Int

  override def handleMessage(socket: DatagramSocket, packet: DatagramPacket): Unit = {

    val objOpt = try {
      getObjectFromDatagramPacket(packet)
    } catch {
      case ex: InvalidProtocolBufferException => None
    }

    objOpt.foreach(obj => {
      val execNoOpt = Option(partitionExecutorMapping.getOrDefault(getKey(obj), -1))
        .map(key => if(key == -1) partitionExecutorMapping.put(getKey(obj), assignPartitionExecutor(getKey(obj))) else key)

      val task = new Runnable {
        override def run(): Unit = {
          try {
            handleRequest(packet.getSocketAddress(), obj)
          } catch {
            case e: SocketException => if(!socket.isClosed) throw e
          }
        }
      }

      execNoOpt.map(execNo => executors(execNo).execute(task))
    })
  }

  override def start() = super.start().asInstanceOf[UdpServer]

  override def stop() = {
    super.stop()
    (0 until executors.size).foreach(ex => executors(ex).shutdown())
  }

}
