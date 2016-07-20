package com.bwsw.tstreams.coordination.pubsub.publisher.actors

import java.net.InetSocketAddress

import akka.actor.Actor
import com.bwsw.tstreams.coordination.pubsub.publisher.actors.BroadcasterConnectionActor.{ChannelInactiveCommand, UpdateSubscribersCommand}
import io.netty.bootstrap.Bootstrap
import io.netty.channel.ChannelId
import org.slf4j.LoggerFactory

class BroadcasterConnectionActor(bootstrap : Bootstrap) extends Actor{
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val idToAddress = scala.collection.mutable.Map[ChannelId, String]()
  private val addressToId = scala.collection.mutable.Map[String, ChannelId]()

  private def channelInactive(id: ChannelId) = {
    if (idToAddress.contains(id)) {
      val address = idToAddress(id)
      idToAddress.remove(id)
      addressToId.remove(address)
    }
  }

  private def connect(subscriber : String) = {
    val splits = subscriber.split(":")
    assert(splits.size == 2)
    val host = splits(0)
    val port = splits(1).toInt
    val channelFuture = bootstrap.connect(new InetSocketAddress(host, port)).sync()
    if (channelFuture.isSuccess){
      idToAddress(channelFuture.channel().id()) = subscriber
      addressToId(subscriber) = channelFuture.channel().id()
    }
  }

  private def updateSubscribers(newSubscribers : List[String]): Unit = {
    logger.debug(s"[BROADCASTER] start updating subscribers:{${addressToId.keys.mkString(",")}}" +
      s" using newSubscribers:{${newSubscribers.mkString(",")}}\n")
    newSubscribers.diff(addressToId.keys.toList) foreach { subscriber =>
      this.connect(subscriber)
    }
    logger.debug(s"[BROADCASTER] updated subscribers:{${addressToId.keys.mkString(",")}}\n")
  }

  override def receive: Receive = {
    case UpdateSubscribersCommand(newSubscribers) => updateSubscribers(newSubscribers)
    case ChannelInactiveCommand(id) => channelInactive(id)
  }
}

object BroadcasterConnectionActor {
  case class UpdateSubscribersCommand(newSubscribers : List[String])
  case class ChannelInactiveCommand(id : ChannelId)
}