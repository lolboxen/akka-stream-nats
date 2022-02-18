package com.lolboxen.nats

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Keep, Sink, Source}
import io.nats.client.{Message, PullSubscribeOptions}
import io.nats.client.impl.NatsMessage

import scala.concurrent.ExecutionContextExecutor

object App {

  implicit val system: ActorSystem = ActorSystem("test")
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  def main(args: Array[String]): Unit = {
    consumer()
//    producer()
  }

  def producer(): Unit = {
    val sink = Producer.jetStreamSinkWithContext[NotUsed]("localhost:4222")
    val (queue, onDone) =
      Source.queue(10000, OverflowStrategy.dropNew)
        .toMat(sink)(Keep.both)
        .run()

    onDone.onComplete(x => println(s"producer closed $x"))(system.dispatcher)

    for(i <- 1 to 10000000) {
      Thread.sleep(1)
      val message = NatsMessage.builder().subject("hive.events.test").data(s"{\"id\":$i}").build()
      queue.offer(message -> NotUsed)
    }
  }

  def consumer(): Unit = {
    val (left, onDone) = Consumer.jetStreamSource("localhost:4222", "hive.events.test", PullSubscribeOptions.bind("apollo", "test"))
      .wireTap(x => println(new String(x.getData))).toMat(Sink.foreach[Message](x => {
        Thread.sleep(1000)
        x.ack()
      }))(Keep.both)
      .run()

    onDone.onComplete(x => println(s"consumer closed $x"))(system.dispatcher)
  }
}
