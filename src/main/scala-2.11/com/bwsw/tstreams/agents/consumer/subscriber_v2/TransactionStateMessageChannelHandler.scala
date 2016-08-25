package com.bwsw.tstreams.agents.consumer.subscriber_v2

import com.bwsw.tstreams.common.ProtocolMessageSerializer
import com.bwsw.tstreams.coordination.messages.master.IMessage
import com.bwsw.tstreams.coordination.messages.state.TransactionStateMessage
import io.netty.channel.{ChannelHandler, ChannelHandlerContext, SimpleChannelInboundHandler}
import io.netty.util.ReferenceCountUtil

import scala.collection.mutable

/**
  * Created by ivan on 25.08.16.
  */
@ChannelHandler.Sharable
class TransactionStateMessageChannelHandler(txnBufferWorkers: mutable.Map[Int, TransactionBufferWorker]) extends SimpleChannelInboundHandler[String] {

  private val partitionCache = mutable.Map[Int, TransactionBufferWorker]()

  override def channelRead0(ctx: ChannelHandlerContext, msg: String): Unit = {
    val m = ProtocolMessageSerializer.deserialize[TransactionStateMessage](msg)
    if (partitionCache.contains(m.partition))
      partitionCache(m.partition)
        .updateAndNotify(TransactionState(uuid              = m.txnUuid,
                                          partition         = m.partition,
                                          masterSessionID   = m.masterID,
                                          queueOrderID      = m.orderID,
                                          itemCount   = m.count,
                                          state       = m.status,
                                          ttl         = m.ttl))
    ReferenceCountUtil.release(msg)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) = {
    IMessage.logger.error(cause.getMessage)
  }
}

