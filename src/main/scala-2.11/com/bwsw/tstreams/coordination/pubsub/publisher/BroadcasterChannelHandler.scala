package com.bwsw.tstreams.coordination.pubsub.publisher

import java.util
import java.util.concurrent.locks.ReentrantLock
import com.bwsw.tstreams.common.serializer.JsonSerializer
import com.bwsw.tstreams.coordination.pubsub.messages.ProducerTopicMessage
import io.netty.channel._
import io.netty.channel.group.DefaultChannelGroup
import io.netty.handler.codec.MessageToMessageEncoder
import io.netty.util.concurrent.GlobalEventExecutor
import org.slf4j.LoggerFactory

/**
 * Broadcaster accepted connection manager
 * @param broadcaster Broadcaster link
 */
@ChannelHandler.Sharable
class BroadcasterChannelHandler(broadcaster : Broadcaster) extends SimpleChannelInboundHandler[ProducerTopicMessage] {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val group = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE)
  private val idToAddress = scala.collection.mutable.Map[ChannelId, String]()
  private val addressToId = scala.collection.mutable.Map[String, ChannelId]()
  private val lock = new ReentrantLock(true)

  /**
   * Triggered on read from channel (incorrect state because broadcaster must only broadcast)
   */
  override def channelRead0(ctx: ChannelHandlerContext, msg: ProducerTopicMessage): Unit = {
    throw new IllegalStateException("Broadcaster must only broadcast messages without any response")
  }

  /**
   * Triggered on connect to new subscriber
   * @param ctx Netty ctx
   */
  override def channelActive(ctx: ChannelHandlerContext) : Unit = {
    group.add(ctx.channel())
  }

  /**
   * Triggered on disconnect of subscriber
   * @param ctx Netty ctx
   */
  override def channelInactive(ctx: ChannelHandlerContext) : Unit = {
    lock.lock()
    val id = ctx.channel().id()
    val address = idToAddress(id)
    idToAddress.remove(id)
    addressToId.remove(address)
    lock.unlock()
  }

  /**
   * Triggered on exception
   * @param ctx Netty ctx
   * @param cause Cause of exception
   */
  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) = {
    cause.printStackTrace()
    ctx.close()
  }

  /**
   * Broadcast msg to all subscribers
   * @param msg Msg to broadcast
   */
  def broadcast(msg : ProducerTopicMessage) : Unit = {
    logger.debug(s"[BROADCASTER PUBLISH] partition=${msg.partition} status=${msg.status} uuid=${msg.txnUuid.timestamp()}\n")
    group.writeAndFlush(msg).await()
  }

  /**
   * Update subscribers with list of new subscribers
   */
  def updateSubscribers(newSubscribers : List[String]): Unit = {
    lock.lock()
    logger.debug(s"[BROADCASTER] start updating subscribers:{${addressToId.keys.mkString(",")}}" +
      s" using newSubscribers:{${newSubscribers.mkString(",")}}\n")
    newSubscribers.diff(addressToId.keys.toList) foreach { subscriber =>
      broadcaster.connect(subscriber)
    }
    logger.debug(s"[BROADCASTER] updated subscribers:{${addressToId.keys.mkString(",")}}, current group size: {${group.size()}}\n")
    lock.unlock()
  }

  /**
   * Update [[idToAddress]]] [[addressToId]]] with new values
   * @param channelId Netty channel id
   * @param address subscriber address
   */
  def updateMap(channelId: ChannelId, address : String) = {
    lock.lock()
    idToAddress(channelId) = address
    addressToId(address) = channelId
    lock.unlock()
  }
}

/**
 * Encoder [[ProducerTopicMessage]]] to [[java.lang.String]]]
 */
class ProducerTopicMessageEncoder extends MessageToMessageEncoder[ProducerTopicMessage]{
  val serializer = new JsonSerializer

  override def encode(ctx: ChannelHandlerContext, msg: ProducerTopicMessage, out: util.List[AnyRef]): Unit = {
    out.add(serializer.serialize(msg) + "\r\n")
  }
}