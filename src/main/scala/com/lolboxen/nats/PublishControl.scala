package com.lolboxen.nats

import akka.Done

import scala.concurrent.{Future, Promise}

trait PublishControl {
  def complete(): Unit

  def whenDone: Future[Done]
}

class NullPublishControl extends PublishControl {
  private val promise: Promise[Done] = Promise()

  override def complete(): Unit = promise.trySuccess(Done)

  override def whenDone: Future[Done] = promise.future
}

class DefaultPublishControl(val completeFn: () => Unit, val whenDone: Future[Done]) extends PublishControl {
  override def complete(): Unit = completeFn()
}
