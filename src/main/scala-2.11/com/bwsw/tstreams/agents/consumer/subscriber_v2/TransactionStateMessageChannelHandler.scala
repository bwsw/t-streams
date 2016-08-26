package com.bwsw.tstreams.agents.consumer.subscriber_v2

import com.bwsw.tstreams.common.ProtocolMessageSerializer
import com.bwsw.tstreams.common.ProtocolMessageSerializer.ProtocolMessageSerializerException
import com.bwsw.tstreams.coordination.messages.master.IMessage
import com.bwsw.tstreams.coordination.messages.state.TransactionStateMessage
import io.netty.channel.{ChannelHandler, ChannelHandlerContext, SimpleChannelInboundHandler}
import io.netty.util.ReferenceCountUtil

import scala.collection.mutable

/**
  * Created by Ivan Kudryavtsev on 25.08.16.
  * Handler for netty which handles updates from producers.
  */
@ChannelHandler.Sharable
class TransactionStateMessageChannelHandler(txnBufferWorkers: mutable.Map[Int, TransactionBufferWorker]) extends SimpleChannelInboundHandler[String] {

  private val partitionCache = mutable.Map[Int, TransactionBufferWorker]()

  txnBufferWorkers
    .foreach(id_w => id_w._2.getPartitions().foreach(p => partitionCache(p) = id_w._2))

  override def channelRead0(ctx: ChannelHandlerContext, msg: String): Unit = {
    try {
      val m = ProtocolMessageSerializer.deserialize[TransactionStateMessage](msg)
      if (partitionCache.contains(m.partition))
        partitionCache(m.partition)
          .updateAndNotify(TransactionState(uuid              = m.txnUuid,
            partition         = m.partition,
            masterSessionID   = m.masterID,
            queueOrderID      = m.orderID,
            itemCount         = m.count,
            state             = m.status,
            ttl               = m.ttl))
      else
        Subscriber.logger.warn(s"Unknown partition ${m.partition} found in Message: ${msg}.")
    } catch {
      case e: ProtocolMessageSerializerException =>
    }
    ReferenceCountUtil.release(msg)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) = {
    Subscriber.logger.warn(cause.getMessage)
  }
}

