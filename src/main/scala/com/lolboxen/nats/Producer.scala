package com.lolboxen.nats

import akka.{Done, NotUsed}
import akka.stream.scaladsl.{Flow, Keep, Sink}
import io.nats.client.Message
import io.nats.client.api.PublishAck

import scala.concurrent.Future

object Producer {
  def jetStreamFlowWithContext[Context](url: String, parallelism: Int = 1): Flow[(Message, Context), (PublishAck, Message, Context), NotUsed] = {
    val natsConnection = new NatsConnection(url)
    val adapter = new JetStreamPublishAdapter(natsConnection)
    Flow.fromGraph(new PublishFlow[Context](adapter))
      .mapAsync(parallelism)(identity)
  }

  def jetStreamSinkWithContext[Context](url: String, parallelism: Int = 1): Sink[(Message, Context), Future[Done]] = {
    jetStreamFlowWithContext(url, parallelism)
      .toMat(Sink.ignore)(Keep.right)
  }

  def jetStreamFlow(url: String, parallelism: Int = 1): Flow[Message, (PublishAck, Message), NotUsed] = {
    val natsConnection = new NatsConnection(url)
    val adapter = new JetStreamPublishAdapter(natsConnection)
    Flow[Message].map(x => x -> NotUsed)
      .via(new PublishFlow[NotUsed](adapter))
      .mapAsync(parallelism)(identity)
      .map(x => (x._1, x._2))
  }

  def jetStreamSink(url: String, parallelism: Int = 1): Sink[Message, Future[Done]] = {
    jetStreamFlow(url, parallelism)
      .toMat(Sink.ignore)(Keep.right)
  }
}
