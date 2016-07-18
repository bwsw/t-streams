package com.bwsw.tstreams.coordination.pubsub.listener.actors

import akka.actor.{ActorRef, ActorSystem, Props}
import com.bwsw.tstreams.coordination.pubsub.messages.ProducerTopicMessage
import akka.pattern.ask
import akka.util.Timeout
import com.bwsw.tstreams.coordination.pubsub.listener.actors.SubscriberActor._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps


class SubscriberManager (system : ActorSystem) {
  private val handler: ActorRef = system.actorOf(
    props = Props[SubscriberActor])

  private val AWAIT_TIMEOUT = 30 seconds
  implicit val asTimeout = Timeout(AWAIT_TIMEOUT)

  def addCallback(callback : (ProducerTopicMessage) => Unit) = {
    handler ! AddCallbackCommand(callback)
  }

  def getCount() : Int = {
    val countFuture = handler ? GetCountCommand
    val count = Await.result(countFuture, asTimeout.duration).asInstanceOf[GetCountResponse].count
    count
  }

  def resetCount() : Unit = {
    handler ! ResetCountCommand
  }

  def invokeCallbacks(msg : ProducerTopicMessage) : Unit = {
    handler ! InvokeCallbackCommand(msg)
  }

  def incrementCount() : Unit = {
    handler ! IncrementCountCommand
  }
}
