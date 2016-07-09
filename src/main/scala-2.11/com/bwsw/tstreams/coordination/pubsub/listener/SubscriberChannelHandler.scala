package com.bwsw.tstreams.coordination.pubsub.listener

import java.util
import java.util.UUID
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreams.common.serializer.JsonSerializer
import com.bwsw.tstreams.coordination.pubsub.messages.{ProducerTopicMessage, ProducerTransactionStatus}
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.JsonMappingException
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import io.netty.handler.codec.MessageToMessageDecoder
import io.netty.util.ReferenceCountUtil
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

/**
 * Incoming connections manager for [[ProducerTopicMessageListener]]]
 */
@Sharable
class SubscriberChannelHandler extends SimpleChannelInboundHandler[ProducerTopicMessage] {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private var count = 0
  private val callbacks = new ListBuffer[(ProducerTopicMessage)=>Unit]()
  private val lockCount = new ReentrantLock(true)
  private val queue = new LinkedBlockingQueue[ProducerTopicMessage]()
  private var callbackThread : Thread = null
  private val isCallback = new AtomicBoolean(false)
  private val lock = new ReentrantLock(true)

  /**
   * Add new callback which is used to handle incoming events([[ProducerTopicMessage]]])
   * @param callback Event callback
   */
  def addCallback(callback : (ProducerTopicMessage)=>Unit) : Unit = {
    lock.lock()
    callbacks += callback
    lock.unlock()
  }

  /**
   * Start callback on incoming events [[ProducerTopicMessage]]]
   */
  def startCallBack() = {
    val sync = new CountDownLatch(1)
    isCallback.set(true)
    callbackThread = new Thread(new Runnable {
      override def run(): Unit = {
        sync.countDown()
        while(isCallback.get()) {
          val msg = queue.take()
          lock.lock()
          callbacks.foreach(x => x(msg))
          lock.unlock()
        }
      }
    })
    callbackThread.start()
    sync.await()
  }

  /**
   * Stop thread which is consuming incoming events [[ProducerTopicMessage]]]
   */
  def stopCallback() : Unit = {
    if (isCallback.get()) {
      isCallback.set(false)
      queue.add(ProducerTopicMessage(UUID.randomUUID(), 0, ProducerTransactionStatus.cancelled, -1))
      callbackThread.join()
    }
  }

  /**
   * Get accepted connections amount
   * @return Amount of connections
   */
  def getCount(): Int = {
    lockCount.lock()
    val cnt = count
    lockCount.unlock()
    cnt
  }

  /**
   * Reset connection amount
   */
  def resetCount() : Unit = {
    lockCount.lock()
    count = 0
    lockCount.unlock()
  }

  /**
   * Triggered when new connection accept
   * @param ctx Netty ctx
   */
  override def channelActive(ctx: ChannelHandlerContext) : Unit = {
    lockCount.lock()
    count += 1
    lockCount.unlock()
  }

  /**
   * Triggered when new message [[ProducerTopicMessage]]] received
   * @param ctx Netty ctx
   * @param msg [[ProducerTopicMessage]]]
   */
  override def channelRead0(ctx: ChannelHandlerContext, msg: ProducerTopicMessage): Unit = {
    logger.debug(s"[READ PARTITION_${msg.partition}] ts=${msg.txnUuid.timestamp()} ttl=${msg.ttl} status=${msg.status}\n")
    queue.put(msg)
    ReferenceCountUtil.release(msg)
  }

  /**
   * Triggered on exceptions
   * @param ctx Netty ctx
   * @param cause Exception cause
   */
  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) = {
    cause.printStackTrace()
    ctx.close()
  }
}

/**
 * Decoder to convert [[java.lang.String]] to [[ProducerTopicMessage]]]
 */
class ProducerTopicMessageDecoder extends MessageToMessageDecoder[String]{
  private val logger = LoggerFactory.getLogger(this.getClass)
  val serializer = new JsonSerializer

  override def decode(ctx: ChannelHandlerContext, msg: String, out: util.List[AnyRef]): Unit = {
    try {
      if (msg != null)
        out.add(serializer.deserialize[ProducerTopicMessage](msg))
    }
    catch {
      case e @ (_:  JsonMappingException | _: JsonParseException) =>
        logger.warn(s"wrong serialization for msg : $msg\n")
    }
  }
}
