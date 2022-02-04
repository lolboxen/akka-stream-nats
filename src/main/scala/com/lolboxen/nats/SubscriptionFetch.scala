package com.lolboxen.nats

import io.nats.client.Message

import scala.concurrent.Future

trait SubscriptionFetch {
  def cancel(): Unit

  def fetch(batchSize: Int): Future[Seq[Message]]
}
