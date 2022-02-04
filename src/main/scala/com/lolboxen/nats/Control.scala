package com.lolboxen.nats

import akka.Done

import scala.concurrent.{ExecutionContext, Future}

object Control {
  def combine(controls: Control*)(implicit ec: ExecutionContext): Control = new Control {
    override def shutdown(): Unit = controls.foreach(_.shutdown())

    override def whenShutdown: Future[Done] = Future.sequence(controls.map(_.whenShutdown)).map(_ => Done)
  }
}

trait Control {
  def shutdown(): Unit

  def whenShutdown: Future[Done]
}
