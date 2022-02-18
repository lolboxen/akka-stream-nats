package com.lolboxen.nats

import io.nats.client.{Connection, PullSubscribeOptions}

class JetStreamPullSubscriptionAdapter(natsConnection: NatsConnection,
                                       subject: String,
                                       pullSubscribeOptions: PullSubscribeOptions)
  extends BaseSubscriptionAdapter(natsConnection, subject) {
  override protected def createSubscriptionFetch(connection: Connection): SubscriptionFetch =
    new JetStreamSubscriptionFetch(connection.jetStream().subscribe(subject, pullSubscribeOptions))

  override protected def subscriptionInfo: String =
    s"subject: $subject stream: ${pullSubscribeOptions.getStream} durable: ${pullSubscribeOptions.getDurable}"
}

