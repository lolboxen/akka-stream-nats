package com.lolboxen.nats

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic}
import com.lolboxen.nats.ConnectionSource.Protocol
import io.nats.client.{Connection, ConsumeOptions, IterableConsumer, JetStreamOptions, Message}
import io.nats.client.api.OrderedConsumerConfiguration

import java.io.IOException
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters.*

class JetStreamOrderedConsumerSource(streamName: String,
                                     config: OrderedConsumerConfiguration,
                                     consumeOptions: ConsumeOptions,
                                     fetchExecutionContext: ExecutionContext,
                                     jetStreamOptions: JetStreamOptions)
  extends GraphStage[FlowShape[Protocol, Message]] {

  protected val in: Inlet[Protocol] = Inlet("JetStreamOrderedConsumerSource.in")
  protected val out: Outlet[Message] = Outlet("JetStreamOrderedConsumerSource.out")
  override def shape: FlowShape[Protocol, Message] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    JetStreamOrderedConsumerSourceLogic(
      streamName,
      config,
      consumeOptions,
      fetchExecutionContext,
      jetStreamOptions,
      inheritedAttributes,
      shape)
}

class JetStreamOrderedConsumerSourceLogic(streamName: String,
                                          config: OrderedConsumerConfiguration,
                                          consumeOptions: ConsumeOptions,
                                          fetchExecutionContext: ExecutionContext,
                                          jetStreamOptions: JetStreamOptions,
                                          inheritedAttributes: Attributes,
                                          shape: FlowShape[Protocol, Message])
  extends PullSubscriptionLogic[IterableConsumer](fetchExecutionContext, shape, inheritedAttributes) {

  override protected def name: Option[String] = {
    val consumers = config.getFilterSubjects.asScala.mkString(",")
    if consumers.isEmpty then Some(streamName)
    else Some(s"$streamName:$consumers")
  }

  override protected def subscribe(connection: Connection): IterableConsumer =
    connection.getStreamContext(streamName, jetStreamOptions)
      .createOrderedConsumer(config)
      .iterate(consumeOptions)

  override protected def unsubscribe(subscription: IterableConsumer): Unit = subscription.close()

  override protected def retryAfterDisconnect(cause: Throwable): Boolean = cause.isInstanceOf[IOException]

  override protected def startPull(subscription: IterableConsumer): Seq[Message] =
    Option(subscription.nextMessage(1000)).toSeq
}
