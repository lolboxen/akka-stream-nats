package com.lolboxen.nats

import akka.stream.stage.{GraphStage, GraphStageLogic}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.lolboxen.nats.ConnectionSource.Protocol
import io.nats.client.{Connection, Message, Subscription}

import scala.concurrent.ExecutionContext

class CoreSubscriptionSource(subject: String, executionContext: ExecutionContext)
  extends GraphStage[FlowShape[Protocol, Message]] {

  protected val in: Inlet[Protocol] = Inlet("CoreSubscriptionSource.in")
  protected val out: Outlet[Message] = Outlet("CoreSubscriptionSource.out")
  override def shape: FlowShape[Protocol, Message] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    CoreSubscriptionSourceLogic(subject, executionContext, inheritedAttributes, shape)
}

class CoreSubscriptionSourceLogic(subject: String,
                                  executionContext: ExecutionContext,
                                  inheritedAttributes: Attributes,
                                  shape: FlowShape[Protocol, Message])
  extends PullSubscriptionLogic[Subscription](executionContext, shape, inheritedAttributes) {

  override protected def name: Option[String] = Some(subject)

  override protected def subscribe(connection: Connection): Subscription = connection.subscribe(subject)

  override protected def unsubscribe(subscription: Subscription): Unit =
    if subscription.isActive then subscription.unsubscribe()

  override protected def startPull(subscription: Subscription): Seq[Message] =
    Option(subscription.nextMessage(1000)).toSeq
}
