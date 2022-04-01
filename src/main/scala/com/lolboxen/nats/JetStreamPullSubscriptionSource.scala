package com.lolboxen.nats

import akka.stream.stage.{GraphStage, GraphStageLogic}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.lolboxen.nats.ConnectionSource.Protocol
import io.nats.client._
import io.nats.client.support.NatsJetStreamConstants

import java.time.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class JetStreamPullSubscriptionSource(subject: String,
                                      fetchSize: Int,
                                      fetchTimeout: Duration,
                                      fetchExecutionContext: ExecutionContext,
                                      jetStreamOptions: JetStreamOptions,
                                      pullOptions: PullSubscribeOptions) extends GraphStage[FlowShape[Protocol, Seq[Message]]] {
  require(fetchSize > 0, "fetchSize must be greater than 0")
  require(fetchSize <= NatsJetStreamConstants.MAX_PULL_SIZE, s"maximum fetchSize is ${NatsJetStreamConstants.MAX_PULL_SIZE}")

  protected val in: Inlet[Protocol] = Inlet("JetStreamPullSubscriptionSource.in")
  protected val out: Outlet[Seq[Message]] = Outlet("JetStreamPullSubscriptionSource.out")
  override def shape: FlowShape[Protocol, Seq[Message]] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new JetStreamPullSubscriptionSourceLogic(
      subject,
      fetchSize,
      fetchTimeout,
      fetchExecutionContext,
      jetStreamOptions,
      pullOptions,
      inheritedAttributes,
      shape
    )
}

class JetStreamPullSubscriptionSourceLogic(subject: String,
                                           fetchSize: Int,
                                           fetchTimeout: Duration,
                                           fetchExecutionContext: ExecutionContext,
                                           jetStreamOptions: JetStreamOptions,
                                           pullOptions: PullSubscribeOptions,
                                           inheritedAttributes: Attributes,
                                           shape: FlowShape[Protocol, Seq[Message]]
                                          ) extends SubscriptionLogic[Seq[Message], JetStreamSubscription](shape, inheritedAttributes) {

  private val fetchResultCallback = getAsyncCallback[Try[Seq[Message]]](fetchResult)

  override protected def subscribe(connection: Connection): JetStreamSubscription = {
    logSubscriptionChange(subject, subscribed = true)
    val sub = connection.jetStream(jetStreamOptions).subscribe(subject, pullOptions)
    if (isAvailable(shape.out)) fetch(sub)
    sub
  }

  override protected def unsubscribe(subscription: JetStreamSubscription): Unit = {
    logSubscriptionChange(subject, subscribed = false)
    if (subscription.isActive) subscription.unsubscribe()
  }

  override def onPull(): Unit = subscription.foreach(fetch)

  private def fetch(sub: JetStreamSubscription): Unit =
    Future {
      sub.fetch(fetchSize, fetchTimeout).asScala.toSeq
    }(fetchExecutionContext).onComplete(fetchResultCallback.invoke)(ExecutionContext.parasitic)

  private def fetchResult(messages: Try[Seq[Message]]): Unit = {
    messages match {
      case Success(value) => push(shape.out, value)
      case Failure(_) => onPull()
    }
  }
}
