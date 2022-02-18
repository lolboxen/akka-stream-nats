package com.lolboxen.nats

import io.nats.client.{Connection, PushSubscribeOptions}

class JetStreamPushSubscriptionAdapter(natsConnection: NatsConnection,
                                       subject: String,
                                       pushSubscribeOptions: PushSubscribeOptions)
  extends BaseSubscriptionAdapter(natsConnection, subject) {
  override protected def createSubscriptionFetch(connection: Connection): SubscriptionFetch = {
    val dispatcher = connection.createDispatcher()
    val handler = new SubscriptionPushFetch
    new JetStreamSubscriptionFetch(connection.jetStream().subscribe(subject, dispatcher, handler, false, pushSubscribeOptions))
  }

  override protected def subscriptionInfo: String =
    s"subject: $subject stream: ${pushSubscribeOptions.getStream} durable: ${pushSubscribeOptions.getDurable}"
}
