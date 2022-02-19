package com.lolboxen.nats

import akka.actor.{ActorSystem, Cancellable}
import akka.stream.scaladsl.{Flow, Keep, Source}
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.{OverflowStrategy, QueueOfferResult}
import io.nats.client.Message
import io.nats.client.api.PublishAck
import io.nats.client.impl.NatsMessage
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future, Promise}
import scala.util.Try

class PublishFlowTest extends AnyFlatSpec with Matchers {

  implicit val system: ActorSystem = ActorSystem()
  val message: Message = NatsMessage.builder().subject("test").data("test").build()

  it should "preserve attached context when message was acked by server" in {
    val support = new TestSupport

    val probe = Source(Seq(message -> "1", message -> "2", message -> "3"))
      .via(Flow.fromGraph(new PublishFlow[String](support)))
      .mapAsync(1)(identity)
      .map(x => (x._2, x._3))
      .runWith(TestSink.probe)

    support.giveth()
    probe
      .ensureSubscription()
      .requestNext(message -> "1")
      .requestNext(message -> "2")
      .requestNext(message -> "3")
      .request(1)
      .expectComplete()
  }

  it should "apply back pressure when publish queue is full" in {
    val support = new TestSupport

    val (queue, _) = Source.queue(1)
      .via(Flow.fromGraph(new PublishFlow[String](support)))
      .toMat(TestSink.probe)(Keep.both)
      .run()

    queue.offer(message -> "1")
    queue.offer(message -> "2")
    queue.offer(message -> "3") shouldBe QueueOfferResult.dropped
    queue.complete()
  }

  it should "respect downstream backpressure" in {
    val support = new TestSupport

    val queue = Source.queue(1)
      .via(Flow.fromGraph(new PublishFlow[String](support)))
      .toMat(TestSink.probe)(Keep.left)
      .run()

    support.giveth()
    queue.offer(message -> "1")
    queue.offer(message -> "2")
    queue.offer(message -> "3") shouldBe QueueOfferResult.dropped
    queue.complete()
  }

  it should "fail when change listener fails" in {
    val support = new TestSupport

    val probe = Source.queue(1, OverflowStrategy.backpressure)
      .via(Flow.fromGraph(new PublishFlow[String](support)))
      .toMat(TestSink.probe)(Keep.right)
      .run()

    probe.ensureSubscription().expectNoMessage(1.second)
    support.fail()
    probe.expectError()
  }

  class TestSupport extends PublishAdapter with Cancellable {

    implicit val ec: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global

    private var cancelCalled = false
    private val listener: Promise[AdapterListener] = Promise()

    override def open(listener: AdapterListener): Unit = {
      this.listener.success(listener)
    }

    override def close(): Unit = ()

    override def apply(message: Message)(implicit ec: ExecutionContext): Future[Option[PublishAck]] = Future.successful(Some(null))

    override def cancel(): Boolean = {
      cancelCalled = true
      true
    }

    override def isCancelled: Boolean = cancelCalled

    def giveth(): Unit = listener.future.map(_.resumeOperations())

    def fail(): Unit = listener.future.map(_.suspendOperationsIndefinitely(new Exception("test induced exception")))

    def expectCancelled(): Unit = assert(cancelCalled)
  }
}
