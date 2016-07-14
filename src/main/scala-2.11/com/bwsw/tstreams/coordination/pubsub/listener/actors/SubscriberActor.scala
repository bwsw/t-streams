package com.bwsw.tstreams.coordination.pubsub.listener.actors

import akka.actor.Actor
import com.bwsw.tstreams.coordination.pubsub.listener.actors.SubscriberActor._
import com.bwsw.tstreams.coordination.pubsub.messages.ProducerTopicMessage

import scala.collection.mutable.ListBuffer


class SubscriberActor extends Actor{
  private val callbacks = new ListBuffer[(ProducerTopicMessage)=>Unit]()
  private var count = 0


  private def addCallback(callback : (ProducerTopicMessage) => Unit) : Unit = {
    callbacks += callback
  }

  private def getCount(): Int = {
    count
  }

  private def resetCount() : Unit = {
    count = 0
  }

  private def incrementCount() : Unit = {
    count += 1
  }

  private def invokeCallbacks(msg : ProducerTopicMessage) : Unit = {
    callbacks.foreach(x=>x(msg))
  }

  override def receive: Receive = {
    case AddCallbackCommand(callback) => addCallback(callback)
    case GetCountCommand => sender ! GetCountResponse(getCount())
    case ResetCountCommand => resetCount()
    case InvokeCallbackCommand(msg) => invokeCallbacks(msg)
    case IncrementCountCommand => incrementCount()
  }
}

object SubscriberActor {
  case class AddCallbackCommand(callback : (ProducerTopicMessage) => Unit)
  case object GetCountCommand
  case class GetCountResponse(count : Int)
  case object ResetCountCommand
  case class InvokeCallbackCommand(msg : ProducerTopicMessage)
  case object IncrementCountCommand
}
