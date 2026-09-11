package com.lolboxen.nats

import akka.stream.scaladsl.Source
import io.nats.client.*
import io.nats.client.api.OrderedConsumerConfiguration

import java.time.Duration
import scala.concurrent.ExecutionContext

object Consumer {
  def coreSource(subject: String,
                 options: Options.Builder,
                 natsExecutionContext: ExecutionContext): Source[Message, Control] =
    Source.fromGraph(ConnectionSource(NatsConnector(options)))
      .via(CoreSubscriptionSource(subject, natsExecutionContext))

  def orderedConsumerSource(streamName: String,
                            options: Options.Builder,
                            config: OrderedConsumerConfiguration,
                            consumeOptions: ConsumeOptions,
                            natsExecutionContext: ExecutionContext,
                            jetStreamOptions: JetStreamOptions): Source[Message, Control] =
    Source.fromGraph(ConnectionSource(NatsConnector(options)))
      .via(JetStreamOrderedConsumerSource(streamName, config, consumeOptions, natsExecutionContext, jetStreamOptions))

  def jetStreamSource(subject: String,
                      autoAck: Boolean,
                      natsExecutionContext: ExecutionContext,
                      options: Options.Builder,
                      jso: JetStreamOptions,
                      pushOptions: PushSubscribeOptions): Source[Message, Control] =
    Source.fromGraph(ConnectionSource(NatsConnector(options)))
      .via(JetStreamPushSubscriptionSource(subject, autoAck, natsExecutionContext, jso, pushOptions))

  def jetStreamSource(subject: String,
                      fetchSize: Int,
                      fetchDuration: Duration,
                      natsExecutionContext: ExecutionContext,
                      options: Options.Builder,
                      jso: JetStreamOptions,
                      pullOptions: PullSubscribeOptions): Source[Message, Control] =
    Source.fromGraph(ConnectionSource(NatsConnector(options)))
      .via(JetStreamPullSubscriptionSource(subject, fetchSize, fetchDuration, natsExecutionContext, jso, pullOptions))
}
