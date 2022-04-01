package com.lolboxen.nats

import akka.stream.stage.{GraphStage, GraphStageLogic}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.lolboxen.nats.ConnectionSource.Protocol
import io.nats.client._

class JetStreamPushSubscriptionSource(subject: String,
                                      autoAck: Boolean,
                                      jetStreamOptions: JetStreamOptions,
                                      pushOptions: PushSubscribeOptions) extends GraphStage[FlowShape[Protocol, Message]] {

  protected val in: Inlet[Protocol] = Inlet("JetStreamPullSubscriptionSource.in")
  protected val out: Outlet[Message] = Outlet("JetStreamPullSubscriptionSource.out")
  override def shape: FlowShape[Protocol, Message] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new JetStreamPushSubscriptionSourceLogic(
      subject,
      autoAck,
      jetStreamOptions,
      pushOptions,
      inheritedAttributes,
      shape
    )
}

class JetStreamPushSubscriptionSourceLogic(subject: String,
                                           autoAck: Boolean,
                                           jetStreamOptions: JetStreamOptions,
                                           pushOptions: PushSubscribeOptions,
                                           inheritedAttributes: Attributes,
                                           shape: FlowShape[Protocol, Message]) extends PushSubscriptionLogic[JetStreamSubscription](shape, inheritedAttributes) {
  override protected def subscribe(connection: Connection): JetStreamSubscription = {
    logSubscriptionChange(subject, subscribed = true)
    val dispatcher = connection.createDispatcher()
    val handler = new MessageHandlerAsync(this)
    connection.jetStream(jetStreamOptions).subscribe(subject, dispatcher, handler, autoAck, pushOptions)
  }

  override protected def unsubscribe(subscription: JetStreamSubscription): Unit = {
    logSubscriptionChange(subject, subscribed = false)
    if (subscription.isActive) subscription.unsubscribe()
  }
}
