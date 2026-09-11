package com.lolboxen.nats

import akka.stream.stage.{GraphStage, GraphStageLogic}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.lolboxen.nats.ConnectionSource.Protocol
import io.nats.client._

import java.time.Duration
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters.*

class JetStreamPullSubscriptionSource(subject: String,
                                      fetchSize: Int,
                                      fetchTimeout: Duration,
                                      executionContext: ExecutionContext,
                                      jetStreamOptions: JetStreamOptions,
                                      pullOptions: PullSubscribeOptions)
  extends GraphStage[FlowShape[Protocol, Message]] {
  require(fetchSize > 0, "fetchSize must be greater than 0")
  require(fetchTimeout != null && fetchTimeout.toMillis > 0, "fetchTimeout must be at least 1 millisecond")

  protected val in: Inlet[Protocol] = Inlet("JetStreamPullSubscriptionSource.in")
  protected val out: Outlet[Message] = Outlet("JetStreamPullSubscriptionSource.out")
  override def shape: FlowShape[Protocol, Message] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    JetStreamPullSubscriptionSourceLogic(
      subject,
      fetchSize,
      fetchTimeout,
      executionContext,
      jetStreamOptions,
      pullOptions,
      inheritedAttributes,
      shape
    )
}

class JetStreamPullSubscriptionSourceLogic(subject: String,
                                           fetchSize: Int,
                                           fetchTimeout: Duration,
                                           executionContext: ExecutionContext,
                                           jetStreamOptions: JetStreamOptions,
                                           pullOptions: PullSubscribeOptions,
                                           inheritedAttributes: Attributes,
                                           shape: FlowShape[Protocol, Message])
  extends PullSubscriptionLogic[JetStreamSubscription](executionContext, shape, inheritedAttributes) {

  override protected def name: Option[String] = Some(subject)

  override protected def subscribe(connection: Connection): JetStreamSubscription =
    connection.jetStream(jetStreamOptions).subscribe(subject, pullOptions)

  override protected def unsubscribe(subscription: JetStreamSubscription): Unit =
    if (subscription.isActive) subscription.unsubscribe()

  override protected def startPull(subscription: JetStreamSubscription): Seq[Message] =
    subscription.fetch(fetchSize, fetchTimeout).asScala.toVector
}
