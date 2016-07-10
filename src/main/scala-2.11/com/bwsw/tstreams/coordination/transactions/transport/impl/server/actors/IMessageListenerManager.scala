package com.bwsw.tstreams.coordination.transactions.transport.impl.server.actors

import java.util.UUID

import akka.actor.{ActorRef, ActorSystem, Props}
import com.bwsw.tstreams.coordination.transactions.messages.IMessage
import com.bwsw.tstreams.coordination.transactions.transport.impl.server.actors.IMessageListenerActor.{AddCallbackCommand, ChannelInactiveCommand, ChannelReadCommand, ResponseCommand}
import io.netty.channel.{Channel, ChannelId}


class IMessageListenerManager(system : ActorSystem) {
  private val handler: ActorRef = system.actorOf(
    props = Props[IMessageListenerActor],
    name  = UUID.randomUUID().toString)

  def addCallback(callback : (IMessage) => Unit) = {
    handler ! AddCallbackCommand(callback)
  }

  def response(msg : IMessage) : Unit = {
    handler ! ResponseCommand(msg)
  }

  def channelRead(address: String, id: ChannelId, channel: Channel, msg : IMessage) = {
    handler ! ChannelReadCommand(address, id, channel, msg)
  }

  def channelInactive(id: ChannelId) = {
    handler ! ChannelInactiveCommand(id)
  }
}
