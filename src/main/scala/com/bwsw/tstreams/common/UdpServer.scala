package com.bwsw.tstreams.common

import java.net._
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Executors}

import com.bwsw.tstreams.proto.protocol.TransactionRequest
import com.google.protobuf.InvalidProtocolBufferException

import collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import scala.util.Random

/**
  * Created by Ivan Kudryavtsev on 20.04.17.
  */
abstract class UdpServer(host: String, port: Int, threads: Int) extends UdpProcessor {
  private val executors = new Array[ExecutorService](threads)
  private val partitionExecutorMapping = new ConcurrentHashMap[Int, Int]()
  private val partitionCounter = new AtomicInteger(0)

  private def assignPartitionExecutor(partition: Int): Int = partitionCounter.getAndIncrement() % threads

  (0 until executors.size).foreach(idx => executors(idx) = Executors.newSingleThreadExecutor())

  override def socketInitializer() = new DatagramSocket(null)

  override def bind(s: DatagramSocket): Unit = {
    socket.bind(new InetSocketAddress(InetAddress.getByName(host), port))
  }

  def handleRequest(client: SocketAddress, req: TransactionRequest)

  override def handleMessage(socket: DatagramSocket, packet: DatagramPacket): Unit = {

    val reqOpt = try {
      Some(TransactionRequest.parseFrom(packet.getData.take(packet.getLength)))
    } catch {
      case ex: InvalidProtocolBufferException => None
    }

    reqOpt.foreach(req => {
      val execNoOpt = Option(partitionExecutorMapping.getOrDefault(req.partition, -1))
        .map(part => if(part == -1) partitionExecutorMapping.put(req.partition, assignPartitionExecutor(req.partition)) else part)

      val task = new Runnable {
        override def run(): Unit = {
          try {
            handleRequest(packet.getSocketAddress(), req)
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
  }

}
