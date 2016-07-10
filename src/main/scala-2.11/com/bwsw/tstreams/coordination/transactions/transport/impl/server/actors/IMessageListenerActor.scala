package com.bwsw.tstreams.coordination.transactions.transport.impl.server.actors

import akka.actor.Actor
import com.bwsw.tstreams.coordination.transactions.messages.IMessage
import com.bwsw.tstreams.coordination.transactions.transport.impl.server.actors.IMessageListenerActor.{AddCallbackCommand, ChannelInactiveCommand, ChannelReadCommand, ResponseCommand}
import io.netty.channel.{Channel, ChannelId}

import scala.collection.mutable.ListBuffer


class IMessageListenerActor extends Actor {
  private val idToChannel = scala.collection.mutable.Map[ChannelId, Channel]()
  private val addressToId = scala.collection.mutable.Map[String, ChannelId]()
  private val idToAddress = scala.collection.mutable.Map[ChannelId, String]()
  private val callbacks = ListBuffer[(IMessage) => Unit]()

  /**
    * Add new event callback on [[IMessage]]]
    * @param callback Event callback
    */
  private def addCallback(callback : (IMessage) => Unit) = {
    callbacks += callback
  }

  /**
    * Response with [[IMessage]]] (it has receiver address)
    */
  private def response(msg : IMessage) : Unit = {
    val responseAddress = msg.receiverID
    if (addressToId.contains(responseAddress)){
      val id = addressToId(responseAddress)
      val channel = idToChannel(id)
      channel.writeAndFlush(msg)
    }
  }

  /**
    *
    * @param address
    * @param id
    * @param channel
    * @param msg
    */
  private def channelRead(address: String, id: ChannelId, channel: Channel, msg : IMessage) = {
    if (!idToChannel.contains(id)){
      idToChannel(id) = channel
      assert(!addressToId.contains(address))
      addressToId(address) = id
      assert(!idToAddress.contains(id))
      idToAddress(id) = address
    }
    callbacks.foreach(x=>x(msg))
  }

  /**
    *
    * @param id
    * @return
    */
  private def channelInactive(id: ChannelId) = {
    if (idToChannel.contains(id)) {
      idToChannel.remove(id)
      assert(idToAddress.contains(id))
      val address = idToAddress(id)
      idToAddress.remove(id)
      assert(addressToId.contains(address))
      addressToId.remove(address)
    }
  }

  override def receive: Receive = {
    case AddCallbackCommand(callback) => addCallback(callback)
    case ResponseCommand(msg) => response(msg)
    case ChannelReadCommand(address, id, channel, msg) => channelRead(address, id, channel, msg)
    case ChannelInactiveCommand(id) => channelInactive(id)
  }
}

object IMessageListenerActor {
  case class AddCallbackCommand(callback : (IMessage) => Unit)
  case class ResponseCommand(msg : IMessage)
  case class ChannelReadCommand(address: String, id: ChannelId, channel: Channel, msg : IMessage)
  case class ChannelInactiveCommand(id: ChannelId)
}
