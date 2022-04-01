package com.lolboxen.nats

import akka.stream.scaladsl.Source
import io.nats.client._

import java.time.Duration
import scala.concurrent.ExecutionContext

object Consumer {
  def coreSource(subject: String, options: Options.Builder): Source[Message, Control] =
    Source.fromGraph(new ConnectionSource(new NatsConnector(options)))
      .via(new CoreSubscriptionSource(subject))

  def jetStreamSource(subject: String,
                      autoAck: Boolean,
                      options: Options.Builder,
                      jso: JetStreamOptions,
                      pushOptions: PushSubscribeOptions): Source[Message, Control] =
    Source.fromGraph(new ConnectionSource(new NatsConnector(options)))
      .via(new JetStreamPushSubscriptionSource(subject, autoAck, jso, pushOptions))

  def jetStreamSource(subject: String,
                      fetchSize: Int,
                      fetchDuration: Duration,
                      fetchExecutionContext: ExecutionContext,
                      options: Options.Builder,
                      jso: JetStreamOptions,
                      pullOptions: PullSubscribeOptions): Source[Message, Control] =
    Source.fromGraph(new ConnectionSource(new NatsConnector(options)))
      .via(new JetStreamPullSubscriptionSource(subject, fetchSize, fetchDuration, fetchExecutionContext, jso, pullOptions))
      .mapConcat(identity)
}
