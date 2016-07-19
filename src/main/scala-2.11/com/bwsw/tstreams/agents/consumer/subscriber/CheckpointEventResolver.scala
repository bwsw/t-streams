package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.UUID
import java.util.concurrent.locks.ReentrantLock

import akka.actor.{ActorRef, ActorSystem, Cancellable, Props}
import akka.util.Timeout
import akka.pattern.ask
import com.bwsw.tstreams.agents.consumer.subscriber.CheckpointEventsResolverActor.{BindBufferCommand, ClearCommand, RefreshCommand, UpdateCommand}
import com.bwsw.tstreams.coordination.pubsub.messages.ProducerTransactionStatus._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.concurrent.ExecutionContext.Implicits.global


class CheckpointEventResolver(subscriber : BasicSubscribingConsumer[_,_])(implicit system : ActorSystem) {
  private var updater : Cancellable = null
  private val UPDATE_INTERVAL = 5000
  private val AWAIT_TIMEOUT = 30 seconds
  implicit val asTimeout = Timeout(AWAIT_TIMEOUT)

  private val handler: ActorRef = system.actorOf(
    props = Props(new CheckpointEventsResolverActor(subscriber)))

  def bindBuffer(partition : Int, buffer : TransactionsBuffer, lock : ReentrantLock) = {
    Await.result(handler ? BindBufferCommand(partition, buffer, lock), asTimeout.duration)
  }

  def update(partition : Int, txn : UUID, status : ProducerTransactionStatus): Future[Any] = {
    val fut = handler ? UpdateCommand(partition, txn, status)
    fut
  }

  def startUpdate() = {
    updater = system.scheduler.schedule(
      initialDelay = 0 milliseconds,
      interval = UPDATE_INTERVAL milliseconds,
      receiver = handler,
      message = RefreshCommand
    )
  }

  def stop() = {
    updater.cancel()
    handler ! ClearCommand
  }
}
