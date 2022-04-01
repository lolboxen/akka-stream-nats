package com.lolboxen.nats

import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Sink}
import akka.stream.{FlowShape, Graph}
import akka.{Done, NotUsed}
import io.nats.client.api.PublishAck
import io.nats.client.{JetStreamOptions, Message, Options}

import scala.concurrent.Future

object Producer {
  def coreFlowWithContext[C](optionsBuilder: Options.Builder): Flow[(Message, C), (Message, C), NotUsed] = {
    Flow.fromGraph(makeGraph[C](optionsBuilder, Publisher.core))
      .mapAsync(1)(identity)
      .map(x => x._2 -> x._3)
  }

  def coreSinkWithContext[C](optionsBuilder: Options.Builder): Sink[(Message, C), Future[Done]] =
    coreFlowWithContext[C](optionsBuilder).toMat(Sink.ignore)(Keep.right)

  def coreFlow(optionsBuilder: Options.Builder): Flow[Message, Message, NotUsed] = {
    Flow[Message]
      .map(_ -> NotUsed)
      .via(coreFlowWithContext[NotUsed](optionsBuilder))
      .map(_._1)
  }

  def coreSink(optionsBuilder: Options.Builder): Sink[Message, Future[Done]] =
    coreFlow(optionsBuilder).toMat(Sink.ignore)(Keep.right)

  def jetStreamAckFlowWithContext[C](optionsBuilder: Options.Builder,
                                     jso: JetStreamOptions,
                                     parallelism: Int): Flow[(Message, C), (PublishAck, Message, C), NotUsed] = {
    require(!jso.isPublishNoAck, "required jso with ack")
    Flow.fromGraph(makeGraph[C](optionsBuilder, Publisher.jetStream(jso)))
      .mapAsync(parallelism)(identity)
      .map { case (ack, message, c) => (ack.get, message, c) }
  }

  def jetStreamAckSinkWithContext[C](optionsBuilder: Options.Builder,
                                     jso: JetStreamOptions,
                                     parallelism: Int): Sink[(Message, C), Future[Done]] = {
    jetStreamAckFlowWithContext[C](optionsBuilder, jso, parallelism)
      .toMat(Sink.ignore)(Keep.right)
  }

  def jetStreamAckFlow(optionsBuilder: Options.Builder,
                       jso: JetStreamOptions,
                       parallelism: Int): Flow[Message, (PublishAck, Message), NotUsed] = {
    Flow[Message].
      map(_ -> NotUsed)
      .via(jetStreamAckFlowWithContext[NotUsed](optionsBuilder, jso, parallelism))
      .map(x => x._1 -> x._2)
  }

  def jetStreamAckSink(optionsBuilder: Options.Builder,
                       jso: JetStreamOptions,
                       parallelism: Int): Sink[Message, Future[Done]] = {
    jetStreamAckFlow(optionsBuilder, jso, parallelism).toMat(Sink.ignore)(Keep.right)
  }

  def jetStreamNoAckFlowWithContext[C](optionsBuilder: Options.Builder,
                                       jso: JetStreamOptions): Flow[(Message, C), (Message, C), NotUsed] = {
    require(jso.isPublishNoAck, "required jso with no ack")
    Flow.fromGraph(makeGraph[C](optionsBuilder, Publisher.jetStream(jso)))
      .mapAsync(1)(identity)
      .map(x => x._2 -> x._3)
  }

  def jetStreamNoAckSinkWithContext[C](optionsBuilder: Options.Builder,
                                       jso: JetStreamOptions): Sink[(Message, C), Future[Done]] = {
    jetStreamNoAckFlowWithContext(optionsBuilder, jso).toMat(Sink.ignore)(Keep.right)
  }

  def jetStreamNoAckFlow(optionsBuilder: Options.Builder,
                         jso: JetStreamOptions): Flow[Message, Message, NotUsed] = {
    Flow[Message]
      .map(_ -> NotUsed)
      .via(jetStreamNoAckFlowWithContext(optionsBuilder, jso))
      .map(x => x._1)
  }

  def jetStreamNoAckSink(optionsBuilder: Options.Builder,
                         jso: JetStreamOptions): Sink[Message, Future[Done]] = {
    jetStreamNoAckFlow(optionsBuilder, jso).toMat(Sink.ignore)(Keep.right)
  }

  private def makeGraph[C](optionsBuilder: Options.Builder,
                           publisherFactory: Publisher.Factory)
  : Graph[FlowShape[(Message, C), Future[(Option[PublishAck], Message, C)]], NotUsed] =
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._
      val connection = builder.add(new ConnectionSource(new NatsConnector(optionsBuilder)))
      val publish = builder.add(new PublishFlow[C](publisherFactory))
      connection.out ~> publish.protocol
      FlowShape(publish.message, publish.out)
    }
}
