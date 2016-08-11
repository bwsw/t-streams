package com.bwsw.tstreams.coordination.producer.transport.impl.server

import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreams.common.TimeTracker
import com.bwsw.tstreams.coordination.messages.master.IMessage
import io.netty.channel.{Channel, ChannelId}

import scala.collection.mutable.ListBuffer


class ProducerRequestsMessageManager() {
  private val idToChannel = scala.collection.mutable.Map[ChannelId, Channel]()
  private val addressToId = scala.collection.mutable.Map[String, ChannelId]()
  private val idToAddress = scala.collection.mutable.Map[ChannelId, String]()
  private val callbacks = ListBuffer[(IMessage) => Unit]()
  private val lock = new ReentrantLock(true)

  def addCallback(callback: (IMessage) => Unit) = {
    lock.lock()
    callbacks += callback
    lock.unlock()
  }

  def respond(msg: IMessage): Unit = {
    lock.lock()
    val responseAddress = msg.receiverID
    if (addressToId.contains(responseAddress)) {
      val id = addressToId(responseAddress)
      val channel = idToChannel(id)
      channel.writeAndFlush(msg)
    }
    lock.unlock()
  }

  def channelRead(address: String, id: ChannelId, channel: Channel, msg: IMessage) = {
    lock.lock()
    if (!idToChannel.contains(id)) {
      idToChannel(id) = channel
      assert(!addressToId.contains(address))
      addressToId(address) = id
      assert(!idToAddress.contains(id))
      idToAddress(id) = address
    }
    callbacks.foreach(x => x(msg))
    lock.unlock()
  }

  def channelInactive(id: ChannelId) = {
    lock.lock()
    if (idToChannel.contains(id)) {
      idToChannel.remove(id)
      assert(idToAddress.contains(id))
      val address = idToAddress(id)
      idToAddress.remove(id)
      assert(addressToId.contains(address))
      addressToId.remove(address)
    }
    lock.unlock()
  }
}
