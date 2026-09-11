package com.lolboxen.nats

import akka.stream.stage.{GraphStage, GraphStageLogic}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.lolboxen.nats.ConnectionSource.Protocol
import io.nats.client.*

import scala.concurrent.ExecutionContext

class JetStreamPushSubscriptionSource(subject: String,
                                      autoAck: Boolean,
                                      executionContext: ExecutionContext,
                                      jetStreamOptions: JetStreamOptions,
                                      pushOptions: PushSubscribeOptions)
  extends GraphStage[FlowShape[Protocol, Message]] {

  protected val in: Inlet[Protocol] = Inlet("JetStreamPullSubscriptionSource.in")
  protected val out: Outlet[Message] = Outlet("JetStreamPullSubscriptionSource.out")
  override def shape: FlowShape[Protocol, Message] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    JetStreamPushSubscriptionSourceLogic(
      subject,
      autoAck,
      executionContext,
      jetStreamOptions,
      pushOptions,
      inheritedAttributes,
      shape
    )
}

class JetStreamPushSubscriptionSourceLogic(subject: String,
                                           autoAck: Boolean,
                                           executionContext: ExecutionContext,
                                           jetStreamOptions: JetStreamOptions,
                                           pushOptions: PushSubscribeOptions,
                                           inheritedAttributes: Attributes,
                                           shape: FlowShape[Protocol, Message])
  extends PushSubscriptionLogic[JetStreamSubscription](executionContext, shape, inheritedAttributes) {
  
  override protected def name: Option[String] = Some(subject)
  
  override protected def subscribe(connection: Connection): JetStreamSubscription = {
    val dispatcher = connection.createDispatcher()
    val handler = MessageHandlerAsync(this)
    connection.jetStream(jetStreamOptions).subscribe(subject, dispatcher, handler, autoAck, pushOptions)
  }

  override protected def unsubscribe(subscription: JetStreamSubscription): Unit =
    if (subscription.isActive) subscription.unsubscribe()
}
