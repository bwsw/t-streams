package com.bwsw.tstreams.common

import java.net._
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Executors}

import com.google.protobuf.InvalidProtocolBufferException

import scala.collection.JavaConverters._

/**
  * Created by Ivan Kudryavtsev on 20.04.17.
  */
abstract class UdpServer(host: String, port: Int, threads: Int) extends UdpProcessor {
  protected val executors = new Array[ExecutorService](threads)
  protected val keyExecutorMapping = new ConcurrentHashMap[Int, Int]()
  private val partitionCounter = new AtomicInteger(0)

  private val keyCounterMap = new ConcurrentHashMap[Int /* key */, AtomicLong /* counter */]()
  private val executorCounterMap = new ConcurrentHashMap[Int /* execNo */, AtomicLong /* counter */]()
  private val executorTaskTimeMap = new ConcurrentHashMap[Int /* execNo */, AtomicLong /* counter */ ]()

  protected def assignPartitionExecutor(partition: Int): Int = partitionCounter.getAndIncrement() % threads

  (0 until executors.size).foreach(idx => {
    executors(idx) = Executors.newSingleThreadExecutor()
    executorCounterMap.put(idx, new AtomicLong(0))
    executorTaskTimeMap.put(idx, new AtomicLong(0))
  })

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
      val objKey = getKey(obj)
      if(keyCounterMap.getOrDefault(objKey, null) == null)
        keyCounterMap.put(getKey(obj), new AtomicLong(0))

      val execNoOpt = Option(keyExecutorMapping.getOrDefault(objKey, -1))
        .map(execNo => if(execNo == -1) keyExecutorMapping.put(objKey, assignPartitionExecutor(objKey)) else execNo)

      val task = new Runnable {
        override def run(): Unit = {
          try {
            val begin = System.nanoTime()
            handleRequest(packet.getSocketAddress(), obj)
            val end = System.nanoTime()
            executorTaskTimeMap.get(execNoOpt.get).addAndGet(end - begin)
          } catch {
            case e: SocketException => if(!socket.isClosed) throw e
          }
        }
      }

      executorCounterMap.get(execNoOpt.get).incrementAndGet()
      keyCounterMap.get(objKey).incrementAndGet()

      execNoOpt.map(execNo => executors(execNo).execute(task))
    })
  }

  override def start() = super.start().asInstanceOf[UdpServer]

  override def stop() = {
    super.stop()
    (0 until executors.size).foreach(ex => executors(ex).shutdown())

    // dump counters
    for(k <- executorCounterMap.keys().asScala)
      if(executorCounterMap.get(k).get() > 0)
        logger.info(s"Executor ${k} processed ${executorCounterMap.get(k).get()} messages. " +
          s" Total time spent ${executorTaskTimeMap.get(k).get() / 1000000} ms, avg per query ${executorTaskTimeMap.get(k).get() * 1.0f / executorCounterMap.get(k).get() / 1000000}")

    for(k <- keyCounterMap.keys().asScala)
      if(keyCounterMap.get(k).get() > 0)
        logger.info(s"Key ${k} (Executor ${keyExecutorMapping.get(k)}) received ${keyCounterMap.get(k).get()} messages.")

  }

}
