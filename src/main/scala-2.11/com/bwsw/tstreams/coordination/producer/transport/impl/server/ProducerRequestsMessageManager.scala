package com.bwsw.tstreams.coordination.producer.transport.impl.server

import com.bwsw.tstreams.coordination.messages.master.IMessage
import io.netty.channel.{Channel, ChannelId}

import scala.collection.mutable.ListBuffer


class ProducerRequestsMessageManager() {
  private val idToChannel = scala.collection.mutable.Map[ChannelId, Channel]()
  private val addressToId = scala.collection.mutable.Map[String, ChannelId]()
  private val idToAddress = scala.collection.mutable.Map[ChannelId, String]()
  private val callbacks = ListBuffer[(IMessage) => Unit]()

  def addCallback(callback: (IMessage) => Unit) = this.synchronized {
    callbacks += callback
  }

  def respond(msg: IMessage): Unit = this.synchronized {
    val responseAddress = msg.receiverID
    if (addressToId.contains(responseAddress)) {
      val id = addressToId(responseAddress)
      val channel = idToChannel(id)
      channel.writeAndFlush(msg, channel.voidPromise())
    }
  }

  def channelRead(address: String, id: ChannelId, channel: Channel, msg: IMessage) = this.synchronized {
    if (!idToChannel.contains(id)) {
      idToChannel(id) = channel
      assert(!addressToId.contains(address))
      addressToId(address) = id
      assert(!idToAddress.contains(id))
      idToAddress(id) = address
    }
    callbacks.foreach(x => x(msg))
  }

  def channelInactive(id: ChannelId) = this.synchronized {
    if (idToChannel.contains(id)) {
      idToChannel.remove(id)
      assert(idToAddress.contains(id))
      val address = idToAddress(id)
      idToAddress.remove(id)
      assert(addressToId.contains(address))
      addressToId.remove(address)
    }
  }
}
