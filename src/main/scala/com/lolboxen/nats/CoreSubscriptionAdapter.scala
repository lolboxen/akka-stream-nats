package com.lolboxen.nats

import io.nats.client.Connection

class CoreSubscriptionAdapter(natsConnection: NatsConnection,
                              subject: String)
  extends BaseSubscriptionAdapter(natsConnection, subject) {
  override protected def createSubscriptionFetch(connection: Connection): SubscriptionFetch = {
    val subscription = new CoreSubscriptionFetch
    connection.createDispatcher(subscription).subscribe(subject)
    subscription
  }

  override protected def subscriptionInfo: String = s"subject $subject"
}