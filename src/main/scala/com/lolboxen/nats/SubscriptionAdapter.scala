package com.lolboxen.nats

import io.nats.client.Message

import scala.concurrent.Future

trait SubscriptionAdapter {
  def open(listener: AdapterListener): Unit
  def close(): Unit
  def fetch(batchSize: Int): Future[Seq[Message]]
}
