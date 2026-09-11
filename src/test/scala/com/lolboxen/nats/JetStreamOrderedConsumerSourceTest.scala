package com.lolboxen.nats

import akka.actor.ActorSystem
import akka.stream.Attributes
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.TestKit
import com.lolboxen.nats.ConnectionSource.{Connected, Disconnected, Protocol}
import io.nats.client._
import io.nats.client.api.OrderedConsumerConfiguration
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.io.IOException
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.{Await, ExecutionContext, Promise}
import scala.concurrent.duration._

class JetStreamOrderedConsumerSourceTest
  extends TestKit(ActorSystem("JetStreamOrderedConsumerSourceTest"))
    with AnyFlatSpecLike
    with Matchers
    with MockFactory
    with BeforeAndAfterAll {

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  private var cleanups = List.empty[() => Unit]

  private def withCleanup(body: => Unit): Unit = {
    try body
    finally {
      try cleanups.foreach(_.apply())
      finally cleanups = Nil
    }
  }

  private class TestIterable {
    val consumer = mock[IterableConsumer]
    private val opened = AtomicBoolean(false)
    private val closed = Promise[Unit]()
    cleanups ::= (() => { if (opened.get()) Await.result(closed.future, 3.seconds); () })

    def open(): IterableConsumer = { opened.set(true); consumer }
    def expectClose(onClose: () => Unit = () => ()): Unit = {
      (consumer.close _).expects().onCall(() => { onClose(); closed.success(()); () })
    }
    def read(message: Message): Unit = {
      (consumer.nextMessage(_: Long)).expects(options.getExpiresInMillis).returning(message)
    }
  }

  private val config = OrderedConsumerConfiguration()
  private val options = ConsumeOptions.builder().batchSize(2).thresholdPercent(50).expiresIn(1000).build()
  private val jetStreamOptions = JetStreamOptions.defaultOptions()

  private def connectionFor(context: OrderedConsumerContext): Connection = {
    val connection = mock[Connection]
    val stream = mock[StreamContext]
    (connection.getStreamContext(_: String, _: JetStreamOptions))
      .expects("stream", jetStreamOptions).returning(stream)
    (stream.createOrderedConsumer(_: OrderedConsumerConfiguration)).expects(config).returning(context)
    connection
  }

  private class ManualExecutionContext extends ExecutionContext {
    private val tasks = LinkedBlockingQueue[Runnable]()
    override def execute(task: Runnable): Unit = { tasks.add(task); () }
    override def reportFailure(cause: Throwable): Unit = throw cause
    def runNext(): Unit = {
      val task = tasks.poll(3, TimeUnit.SECONDS)
      task should not be null
      task.run()
    }
    def isIdle: Boolean = tasks.isEmpty
    def pendingTasks: Int = tasks.size()
    def expectIdle(): Unit = tasks.poll(100, TimeUnit.MILLISECONDS) shouldBe null
  }

  private def materialize(executionContext: ExecutionContext) = {
    val ((pub, stopped), sub) = TestSource[Protocol]()
      .via(JetStreamOrderedConsumerSource("stream", config, options, executionContext, jetStreamOptions))
      .watchTermination()(Keep.both)
      .toMat(TestSink[Message]())(Keep.both)
      .withAttributes(Attributes.inputBuffer(1, 1))
      .run()
    cleanups ::= (() => { sub.cancel(); Await.ready(stopped, 3.seconds); () })
    (pub, sub)
  }

  it should "create the iterable on connection without reading before demand" in withCleanup {
    val context = mock[OrderedConsumerContext]
    val connection = connectionFor(context)
    val iterable = new TestIterable
    (context.iterate(_: ConsumeOptions)).expects(options).onCall((_: ConsumeOptions) => iterable.open())
    iterable.expectClose()
    val ec = ManualExecutionContext()
    val (pub, sub) = materialize(ec)
    sub.ensureSubscription()
    pub.sendNext(Connected(connection))
    pub.expectRequest()
    ec.runNext() // Initialize on the supplied execution context, without demand.
    ec.expectIdle()
    pub.sendComplete()
    sub.expectComplete()
  }

  it should "resume demand received while iterable creation is in flight" in withCleanup {
    val context = mock[OrderedConsumerContext]
    val connection = connectionFor(context)
    val iterable = new TestIterable
    val message = mock[Message]
    (context.iterate(_: ConsumeOptions)).expects(options).onCall((_: ConsumeOptions) => iterable.open())
    iterable.read(message)
    iterable.expectClose()

    val ec = ManualExecutionContext()
    val (pub, sub) = materialize(ec)
    pub.sendNext(Connected(connection))
    pub.expectRequest()
    sub.request(1).expectNoMessage(100.millis)
    ec.pendingTasks shouldBe 1
    ec.runNext() // Subscription creation completes after onPull saw no subscription.
    ec.runNext() // Its completion must schedule the pending read.
    sub.expectNext(message)
    ec.expectIdle()
    pub.sendComplete()
    sub.expectComplete()
  }

  it should "keep only one subscription creation in flight across connection notifications" in withCleanup {
    val context = mock[OrderedConsumerContext]
    val connection = connectionFor(context)
    val iterable = new TestIterable
    (context.iterate(_: ConsumeOptions)).expects(options).onCall((_: ConsumeOptions) => iterable.open())
    iterable.expectClose()

    val ec = ManualExecutionContext()
    val (pub, sub) = materialize(ec)
    sub.ensureSubscription()
    pub.sendNext(Connected(connection))
    pub.expectRequest()
    pub.sendNext(Connected(connection))
    pub.expectRequest()
    pub.sendNext(Disconnected)
    pub.expectRequest()
    pub.sendNext(Connected(connection))
    pub.expectRequest()
    ec.pendingTasks shouldBe 1
    ec.runNext()
    ec.expectIdle()
    pub.sendComplete()
    sub.expectComplete()
  }

  it should "serialize reads on one iterable beyond the configured batch size" in withCleanup {
    val context = mock[OrderedConsumerContext]
    val connection = connectionFor(context)
    val iterable = new TestIterable
    val a = mock[Message]
    val b = mock[Message]
    val c = mock[Message]
    (context.iterate(_: ConsumeOptions)).expects(options).onCall((_: ConsumeOptions) => iterable.open())
    inSequence {
      iterable.read(a)
      iterable.read(b)
      iterable.read(c)
    }
    iterable.expectClose()

    val ec = ManualExecutionContext()
    val (pub, sub) = materialize(ec)
    pub.sendNext(Connected(connection))
    pub.expectRequest()
    ec.runNext() // Complete asynchronous subscription creation.
    sub.request(1)
    awaitCond(!ec.isIdle)
    sub.request(2)
    pub.sendNext(Connected(connection))
    pub.expectRequest()
    ec.pendingTasks shouldBe 1
    for (message <- Seq(a, b, c)) {
      ec.runNext()
      sub.expectNext(message)
    }
    ec.expectIdle()
    // batchSize controls jnats prefetching, not the lifetime of the source.
    sub.expectNoMessage(100.millis)
    pub.sendComplete()
    sub.expectComplete()
  }

  it should "read from the retained iterable only when there is demand" in withCleanup {
    val context = mock[OrderedConsumerContext]
    val connection = connectionFor(context)
    val iterable = new TestIterable
    val a = mock[Message]
    val b = mock[Message]
    (context.iterate(_: ConsumeOptions)).expects(options).onCall((_: ConsumeOptions) => iterable.open())
    inSequence {
      iterable.read(a)
      iterable.read(b)
    }
    iterable.expectClose()

    val ec = ManualExecutionContext()
    val (pub, sub) = materialize(ec)
    sub.ensureSubscription()
    pub.sendNext(Connected(connection))
    pub.expectRequest()
    ec.runNext() // Complete asynchronous subscription creation.
    ec.expectIdle()
    for (message <- Seq(a, b)) {
      sub.request(1)
      ec.runNext()
      sub.expectNext(message).expectNoMessage(100.millis)
      ec.expectIdle()
    }
    pub.sendComplete()
    sub.expectComplete()
  }

  it should "service demand that arrived before connecting" in withCleanup {
    val context = mock[OrderedConsumerContext]
    val connection = connectionFor(context)
    val iterable = new TestIterable
    val message = mock[Message]
    (context.iterate(_: ConsumeOptions)).expects(options).onCall((_: ConsumeOptions) => iterable.open())
    iterable.read(message)
    iterable.expectClose()

    val ec = ManualExecutionContext()
    val (pub, sub) = materialize(ec)
    sub.request(1).expectNoMessage(100.millis)
    pub.sendNext(Connected(connection))
    pub.expectRequest()
    ec.runNext() // Complete asynchronous subscription creation.
    ec.runNext()
    sub.expectNext(message)
    ec.expectIdle()
    pub.sendComplete()
    sub.expectComplete()
  }

  it should "retry timed-out reads on the same iterable without satisfying demand" in withCleanup {
    val context = mock[OrderedConsumerContext]
    val connection = connectionFor(context)
    val iterable = new TestIterable
    val message = mock[Message]
    (context.iterate(_: ConsumeOptions)).expects(options).onCall((_: ConsumeOptions) => iterable.open())
    inSequence {
      iterable.read(null)
      iterable.read(null)
      iterable.read(message)
    }
    iterable.expectClose()

    val ec = ManualExecutionContext()
    val (pub, sub) = materialize(ec)
    pub.sendNext(Connected(connection))
    pub.expectRequest()
    ec.runNext() // Complete asynchronous subscription creation.
    sub.request(1)
    ec.runNext()
    sub.expectNoMessage(100.millis)
    ec.runNext()
    sub.expectNoMessage(100.millis)
    ec.runNext()
    sub.expectNext(message)
    ec.expectIdle()
    pub.sendComplete()
    sub.expectComplete()
  }

  it should "fail the stage and close the iterable when a read fails" in withCleanup {
    val context = mock[OrderedConsumerContext]
    val connection = connectionFor(context)
    val iterable = new TestIterable
    val consumer = iterable.consumer
    val error = IOException("read failed")
    (context.iterate(_: ConsumeOptions)).expects(options).onCall((_: ConsumeOptions) => iterable.open())
    (consumer.nextMessage(_: Long)).expects(options.getExpiresInMillis).throwing(error)
    iterable.expectClose()

    val ec = ManualExecutionContext()
    val (pub, sub) = materialize(ec)
    pub.sendNext(Connected(connection))
    pub.expectRequest()
    ec.runNext() // Complete asynchronous subscription creation.
    sub.request(1)
    ec.runNext()
    sub.expectError() shouldBe error
  }

  it should "fail the stage when creating an iterable fails" in withCleanup {
    val context = mock[OrderedConsumerContext]
    val connection = connectionFor(context)
    val error = IOException("iterate failed")
    (context.iterate(_: ConsumeOptions)).expects(options).throwing(error)

    val ec = ManualExecutionContext()
    val (pub, sub) = materialize(ec)
    pub.sendNext(Connected(connection))
    pub.expectRequest()
    sub.request(1)
    ec.runNext()
    sub.expectError() shouldBe error
  }

  it should "close an iterable that finishes creation after cancellation" in withCleanup {
    val context = mock[OrderedConsumerContext]
    val connection = connectionFor(context)
    val iterable = new TestIterable
    val started = Promise[Unit]()
    val release = Promise[Unit]()
    val closed = Promise[Unit]()
    (context.iterate(_: ConsumeOptions)).expects(options).onCall { (_: ConsumeOptions) =>
      started.success(())
      Await.result(release.future, 3.seconds)
      iterable.open()
    }
    iterable.expectClose(() => { closed.success(()); () })

    val (pub, sub) = materialize(ExecutionContext.global)
    try {
      pub.sendNext(Connected(connection))
      sub.request(10)
      Await.result(started.future, 3.seconds)
      sub.cancel()
      pub.expectCancellation()
    } finally release.trySuccess(())
    Await.result(closed.future, 3.seconds)
  }

  it should "reuse the context and iterable across reconnects without reading while disconnected" in withCleanup {
    val context = mock[OrderedConsumerContext]
    val connection = connectionFor(context)
    val iterable = new TestIterable
    val a = mock[Message]
    val b = mock[Message]
    val ec = ManualExecutionContext()
    (context.iterate(_: ConsumeOptions)).expects(options).onCall((_: ConsumeOptions) => iterable.open())
    inSequence {
      iterable.read(a)
      iterable.read(b)
    }
    iterable.expectClose()

    val (pub, sub) = materialize(ec)
    pub.sendNext(Connected(connection))
    pub.expectRequest()
    ec.runNext() // Complete asynchronous subscription creation.
    sub.request(1)
    ec.runNext()
    sub.expectNext(a)
    pub.sendNext(Disconnected)
    pub.expectRequest()
    sub.request(1)
    ec.expectIdle()
    pub.sendNext(Connected(connection))
    pub.expectRequest()
    ec.runNext()
    sub.expectNext(b)
    ec.expectIdle()
    pub.sendComplete()
    sub.expectComplete()
  }

  for (completeBeforeReconnect <- Seq(true, false)) {
    it should s"retain an in-flight message when it completes ${if (completeBeforeReconnect) "before" else "after"} reconnect" in withCleanup {
      val context = mock[OrderedConsumerContext]
      val connection = connectionFor(context)
      val iterable = new TestIterable
      val message = mock[Message]
      val ec = ManualExecutionContext()
      (context.iterate(_: ConsumeOptions)).expects(options).onCall((_: ConsumeOptions) => iterable.open())
      iterable.read(message)
      iterable.expectClose()

      val (pub, sub) = materialize(ec)
      pub.sendNext(Connected(connection))
      pub.expectRequest()
      ec.runNext() // Complete asynchronous subscription creation.
      sub.request(1)
      // Wait until the read has been submitted before processing the disconnect.
      awaitCond(!ec.isIdle)
      pub.sendNext(Disconnected)
      pub.expectRequest()
      if (completeBeforeReconnect) {
        ec.runNext()
        sub.expectNext(message)
      }
      pub.sendNext(Connected(connection))
      pub.expectRequest()
      if (!completeBeforeReconnect) {
        ec.runNext()
        sub.expectNext(message)
      }
      ec.expectIdle()
      pub.sendComplete()
      sub.expectComplete()
    }
  }

  for (failBeforeReconnect <- Seq(true, false)) {
    it should s"fail on initialization errors received ${if (failBeforeReconnect) "before" else "after"} reconnect" in withCleanup {
      val context = mock[OrderedConsumerContext]
      val connection = connectionFor(context)
      val error = IOException("initialization interrupted")
      val ec = ManualExecutionContext()
      (context.iterate(_: ConsumeOptions)).expects(options).throwing(error)

      val (pub, sub) = materialize(ec)
      sub.ensureSubscription()
      pub.sendNext(Connected(connection))
      pub.expectRequest()
      pub.sendNext(Disconnected)
      pub.expectRequest()
      if (!failBeforeReconnect) {
        pub.sendNext(Connected(connection))
        pub.expectRequest()
      }
      ec.pendingTasks shouldBe 1
      ec.runNext()
      // Subscription creation failures are terminal; reconnect recovery applies
      // to an established consumer, not a failed initialization attempt.
      sub.expectError() shouldBe error
      pub.expectCancellation()
      ec.expectIdle()
    }
  }

  it should "resume on the same iterable after a read times out during disconnect" in withCleanup {
    val context = mock[OrderedConsumerContext]
    val connection = connectionFor(context)
    val iterable = new TestIterable
    val message = mock[Message]
    val ec = ManualExecutionContext()
    (context.iterate(_: ConsumeOptions)).expects(options).onCall((_: ConsumeOptions) => iterable.open())
    inSequence {
      iterable.read(null)
      iterable.read(message)
    }
    iterable.expectClose()

    val (pub, sub) = materialize(ec)
    pub.sendNext(Connected(connection))
    pub.expectRequest()
    ec.runNext() // Complete asynchronous subscription creation.
    sub.request(1)
    awaitCond(!ec.isIdle)
    pub.sendNext(Disconnected)
    pub.expectRequest()
    ec.runNext()
    ec.expectIdle()
    pub.sendNext(Connected(connection))
    pub.expectRequest()
    ec.runNext()
    sub.expectNext(message)
    ec.expectIdle()
    pub.sendComplete()
    sub.expectComplete()
  }

  it should "reject a different Connection without resetting the ordered position" in withCleanup {
    val context = mock[OrderedConsumerContext]
    val connection = connectionFor(context)
    val replacement = mock[Connection]
    val iterable = new TestIterable
    val message = mock[Message]
    (context.iterate(_: ConsumeOptions)).expects(options).onCall((_: ConsumeOptions) => iterable.open())
    iterable.read(message)
    iterable.expectClose()

    val ec = ManualExecutionContext()
    val (pub, sub) = materialize(ec)
    pub.sendNext(Connected(connection))
    pub.expectRequest()
    ec.runNext() // Complete asynchronous subscription creation.
    sub.request(1)
    ec.runNext()
    sub.expectNext(message)
    pub.sendNext(Disconnected)
    pub.expectRequest()
    pub.sendNext(Connected(replacement))
    sub.expectError() shouldBe a[IllegalArgumentException]
  }

  for (cancel <- Seq(true, false)) {
    it should s"close the retained iterable on ${if (cancel) "cancellation" else "completion"} while disconnected" in withCleanup {
      val context = mock[OrderedConsumerContext]
      val connection = connectionFor(context)
      val iterable = new TestIterable
      val message = mock[Message]
      val closed = Promise[Unit]()
      (context.iterate(_: ConsumeOptions)).expects(options).onCall((_: ConsumeOptions) => iterable.open())
      iterable.read(message)
      iterable.expectClose(() => { closed.success(()); () })

      val ec = ManualExecutionContext()
      val (pub, sub) = materialize(ec)
      pub.sendNext(Connected(connection))
      pub.expectRequest()
      ec.runNext() // Complete asynchronous subscription creation.
      sub.request(1)
      ec.runNext()
      sub.expectNext(message)
      pub.sendNext(Disconnected)
      pub.expectRequest()
      closed.isCompleted shouldBe false
      if (cancel) sub.cancel()
      else {
        pub.sendComplete()
        sub.expectComplete()
      }
      Await.result(closed.future, 3.seconds)
    }
  }
}
