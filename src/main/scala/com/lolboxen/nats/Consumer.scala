package com.lolboxen.nats

import akka.stream.scaladsl.Source
import io.nats.client.{Message, PullSubscribeOptions, PushSubscribeOptions}

object Consumer {
  def jetStreamSource(url: String, subject: String, pullSubscribeOptions: PullSubscribeOptions): Source[Message, Control] = {
    val natsConnection = new NatsConnection(url)
    val adapter = new JetStreamPullSubscriptionAdapter(natsConnection, subject, pullSubscribeOptions)
    Source.fromGraph(new SubscriptionSource(adapter))
  }

  def jetStreamSource(url: String, subject: String, pushSubscribeOptions: PushSubscribeOptions): Source[Message, Control] = {
    val natsConnection = new NatsConnection(url)
    val adapter = new JetStreamPushSubscriptionAdapter(natsConnection, subject, pushSubscribeOptions)
    Source.fromGraph(new SubscriptionSource(adapter))
  }

  def coreSource(url: String, subject: String): Source[Message, Control] = {
    val natsConnection = new NatsConnection(url)
    val adapter = new CoreSubscriptionAdapter(natsConnection, subject)
    Source.fromGraph(new SubscriptionSource(adapter))
  }
}
