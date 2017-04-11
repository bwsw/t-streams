package com.bwsw.tstreams.agents.consumer.subscriber

import com.bwsw.tstreams.common.ProtocolMessageSerializer
import com.bwsw.tstreams.common.ProtocolMessageSerializer.ProtocolMessageSerializerException
import com.bwsw.tstreams.coordination.messages.state.TransactionStateMessage
import io.netty.channel.socket.DatagramPacket
import io.netty.channel.{ChannelHandler, ChannelHandlerContext, SimpleChannelInboundHandler}
import io.netty.util.CharsetUtil

import scala.collection.mutable

/**
  * Created by Ivan Kudryavtsev on 25.08.16.
  * Handler for netty which handles updates from producers.
  */
@ChannelHandler.Sharable
class TransactionStateMessageChannelHandler(transactionsBufferWorkers: mutable.Map[Int, TransactionBufferWorker]) extends SimpleChannelInboundHandler[DatagramPacket] {

  private val partitionCache = mutable.Map[Int, TransactionBufferWorker]()

  transactionsBufferWorkers
    .foreach(id_w => id_w._2.getPartitions().foreach(p => partitionCache(p) = id_w._2))

  override def channelRead0(ctx: ChannelHandlerContext, msg: DatagramPacket): Unit = {
    try {
      val m = ProtocolMessageSerializer.deserialize[TransactionStateMessage](msg.content().toString(CharsetUtil.UTF_8))
      if (partitionCache.contains(m.partition))
        partitionCache(m.partition)
          .update(TransactionState(transactionID = m.transactionID,
            partition = m.partition,
            masterSessionID = m.masterID,
            queueOrderID = m.orderID,
            itemCount = m.count,
            state = m.status,
            ttlMs = m.ttlMs))
      else
        Subscriber.logger.warn(s"Unknown partition ${m.partition} found in Message: $msg.")
    } catch {
      case e: ProtocolMessageSerializerException =>
    }
  }

  override def channelReadComplete(ctx: ChannelHandlerContext) = {
    ctx.flush()
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) = {
    Subscriber.logger.warn(cause.getMessage)
  }
}

