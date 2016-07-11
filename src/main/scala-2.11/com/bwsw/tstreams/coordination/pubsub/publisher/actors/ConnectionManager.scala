package com.bwsw.tstreams.coordination.pubsub.publisher.actors

import akka.actor.{ActorRef, ActorSystem, Props}
import com.bwsw.tstreams.coordination.pubsub.publisher.actors.ConnectionActor.{ChannelInactiveCommand, UpdateSubscribersCommand}
import io.netty.bootstrap.Bootstrap
import io.netty.channel.ChannelId

class ConnectionManager(system : ActorSystem, bootstrap : Bootstrap) {
  private val handler: ActorRef = system.actorOf(
    props = Props(new ConnectionActor(bootstrap)))

  def updateSubscribers(newSubscribers : List[String]) = {
    handler ! UpdateSubscribersCommand(newSubscribers)
  }

  def channelInactive(id : ChannelId) = {
    handler ! ChannelInactiveCommand(id)
  }
}
