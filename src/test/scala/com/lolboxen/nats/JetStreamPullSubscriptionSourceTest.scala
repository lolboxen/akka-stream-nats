package com.lolboxen.nats

import akka.actor.ActorSystem
import akka.stream.Attributes
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.TestKit
import com.lolboxen.nats.ConnectionSource.{Connected, Disconnected, Protocol}
import io.nats.client._
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.time.Duration
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import scala.concurrent.{Await, ExecutionContext, Promise}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class JetStreamPullSubscriptionSourceTest
  extends TestKit(ActorSystem("JetStreamPullSubscriptionSourceTest"))
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

  private class ManualExecutionContext extends ExecutionContext {
    private val tasks = new LinkedBlockingQueue[Runnable]()
    override def execute(task: Runnable): Unit = { tasks.add(task); () }
    override def reportFailure(cause: Throwable): Unit = throw cause
    def isIdle: Boolean = tasks.isEmpty
    def pendingTasks: Int = tasks.size()
    def runNext(): Unit = {
      val task = tasks.poll(3, TimeUnit.SECONDS)
      task should not be null
      task.run()
    }
    def expectIdle(): Unit = tasks.poll(100, TimeUnit.MILLISECONDS) shouldBe null
  }

  private class Fixture {
    val connection = mock[Connection]
    val jetStream = mock[JetStream]
    val subscription = mock[JetStreamSubscription]
    val closed = Promise[Unit]()
    val ec = new ManualExecutionContext
    val timeout = Duration.ofMillis(10)
    (connection.jetStream(_: JetStreamOptions)).expects(*).once().returning(jetStream)
    (jetStream.subscribe(_: String, _: PullSubscribeOptions)).expects("subject", *).once().returning(subscription)
    (subscription.isActive _).expects().once().returning(true)
    (subscription.unsubscribe: () => Unit).expects().once().onCall(() => { closed.success(()); () })

    def run() = {
      val ((pub, stopped), sub) = TestSource[Protocol]()
        .via(JetStreamPullSubscriptionSource("subject", 100, timeout, ec,
          JetStreamOptions.defaultOptions(), PullSubscribeOptions.bind("stream", "durable")))
        .watchTermination()(Keep.both)
        .toMat(TestSink[Message]())(Keep.both)
        .withAttributes(Attributes.inputBuffer(1, 1))
        .run()
      cleanups ::= (() => { sub.cancel(); Await.ready(stopped, 3.seconds); Await.result(closed.future, 3.seconds); () })
      (pub, sub)
    }
  }

  it should "fetch only while there is demand, including after draining an entire batch" in withCleanup {
    val f = new Fixture
    val first = mock[Message]
    val second = mock[Message]
    inSequence {
      (f.subscription.fetch(_: Int, _: Duration)).expects(100, f.timeout).returning(List(first).asJava)
      (f.subscription.fetch(_: Int, _: Duration)).expects(100, f.timeout).returning(List(second).asJava)
    }
    val (pub, sub) = f.run()
    pub.sendNext(Connected(f.connection))
    pub.expectRequest()
    f.ec.runNext() // Complete asynchronous subscription creation.
    f.ec.expectIdle()
    sub.request(1)
    f.ec.runNext()
    sub.expectNext(first)
    f.ec.expectIdle()
    sub.request(1)
    f.ec.runNext()
    sub.expectNext(second)
    f.ec.expectIdle()
    pub.sendComplete()
    sub.expectComplete()
  }

  it should "serialize fetches when more demand and duplicate connection notifications arrive" in withCleanup {
    val f = new Fixture
    val first = mock[Message]
    val second = mock[Message]
    inSequence {
      (f.subscription.fetch(_: Int, _: Duration)).expects(100, f.timeout).returning(List(first).asJava)
      (f.subscription.fetch(_: Int, _: Duration)).expects(100, f.timeout).returning(List(second).asJava)
    }
    val (pub, sub) = f.run()
    pub.sendNext(Connected(f.connection))
    pub.expectRequest()
    f.ec.runNext() // Complete asynchronous subscription creation.
    sub.request(1)
    awaitCond(!f.ec.isIdle)
    sub.request(1)
    pub.sendNext(Connected(f.connection))
    pub.expectRequest()
    f.ec.pendingTasks shouldBe 1
    f.ec.runNext()
    sub.expectNext(first)
    awaitCond(!f.ec.isIdle)
    f.ec.pendingTasks shouldBe 1
    f.ec.runNext()
    sub.expectNext(second)
    f.ec.expectIdle()
    pub.sendComplete()
    sub.expectComplete()
  }

  it should "service demand received before connecting" in withCleanup {
    val f = new Fixture
    val message = mock[Message]
    (f.subscription.fetch(_: Int, _: Duration)).expects(100, f.timeout).returning(List(message).asJava)
    val (pub, sub) = f.run()
    sub.request(1)
    f.ec.expectIdle()
    pub.sendNext(Connected(f.connection))
    pub.expectRequest()
    f.ec.runNext() // Complete asynchronous subscription creation.
    f.ec.runNext()
    sub.expectNext(message)
    f.ec.expectIdle()
    pub.sendComplete()
    sub.expectComplete()
  }

  it should "pause fetches while disconnected and resume on the same subscription" in withCleanup {
    val f = new Fixture
    val first = mock[Message]
    val second = mock[Message]
    inSequence {
      (f.subscription.fetch(_: Int, _: Duration)).expects(100, f.timeout).returning(List(first).asJava)
      (f.subscription.fetch(_: Int, _: Duration)).expects(100, f.timeout).returning(List(second).asJava)
    }
    val (pub, sub) = f.run()
    pub.sendNext(Connected(f.connection))
    pub.expectRequest()
    f.ec.runNext() // Complete asynchronous subscription creation.
    f.ec.expectIdle()
    sub.request(1)
    f.ec.runNext()
    sub.expectNext(first)
    pub.sendNext(Disconnected)
    pub.expectRequest()
    sub.request(1)
    f.ec.expectIdle()
    f.closed.isCompleted shouldBe false
    pub.sendNext(Connected(f.connection))
    pub.expectRequest()
    f.ec.runNext()
    sub.expectNext(second)
    pub.sendComplete()
    sub.expectComplete()
  }

  for (completeBeforeReconnect <- Seq(true, false)) {
    it should s"preserve an in-flight batch completed ${if (completeBeforeReconnect) "before" else "after"} reconnect without overlapping fetches" in withCleanup {
      val f = new Fixture
      val message = mock[Message]
      (f.subscription.fetch(_: Int, _: Duration)).expects(100, f.timeout).once().returning(List(message).asJava)
      val (pub, sub) = f.run()
      pub.sendNext(Connected(f.connection))
      pub.expectRequest()
      f.ec.runNext() // Complete asynchronous subscription creation.
      sub.request(1)
      awaitCond(!f.ec.isIdle)
      pub.sendNext(Disconnected)
      pub.expectRequest()
      if (completeBeforeReconnect) {
        f.ec.runNext()
        sub.expectNext(message)
      }
      pub.sendNext(Connected(f.connection))
      pub.expectRequest()
      // Duplicate connection notifications must not replace the subscription or
      // schedule a second fetch for the outstanding downstream pull.
      pub.sendNext(Connected(f.connection))
      pub.expectRequest()
      if (!completeBeforeReconnect) {
        f.ec.runNext()
        sub.expectNext(message)
      }
      f.ec.expectIdle()
      pub.sendComplete()
      sub.expectComplete()
    }
  }

  it should "fail on a fetch error instead of retrying indefinitely" in withCleanup {
    val f = new Fixture
    val error = new IllegalStateException("subscription is inactive")
    (f.subscription.fetch(_: Int, _: Duration)).expects(100, f.timeout).once().throwing(error)
    val (pub, sub) = f.run()
    pub.sendNext(Connected(f.connection))
    pub.expectRequest()
    f.ec.runNext() // Complete asynchronous subscription creation.
    sub.request(1)
    f.ec.runNext()
    sub.expectError() shouldBe error
    f.ec.expectIdle()
    Await.result(f.closed.future, 3.seconds)
  }

  it should "continue after an empty timeout batch" in withCleanup {
    val f = new Fixture
    val message = mock[Message]
    inSequence {
      (f.subscription.fetch(_: Int, _: Duration)).expects(100, f.timeout).returning(List.empty[Message].asJava)
      (f.subscription.fetch(_: Int, _: Duration)).expects(100, f.timeout).returning(List(message).asJava)
    }
    val (pub, sub) = f.run()
    pub.sendNext(Connected(f.connection))
    pub.expectRequest()
    f.ec.runNext() // Complete asynchronous subscription creation.
    sub.request(1)
    f.ec.runNext()
    sub.expectNoMessage(100.millis)
    f.ec.runNext()
    sub.expectNext(message)
    f.ec.expectIdle()
    pub.sendComplete()
    sub.expectComplete()
  }

  for (cancel <- Seq(true, false)) {
    it should s"unsubscribe on ${if (cancel) "cancellation" else "completion"} while disconnected" in withCleanup {
      val f = new Fixture
      val (pub, sub) = f.run()
      sub.ensureSubscription()
      pub.sendNext(Connected(f.connection))
      pub.expectRequest()
      f.ec.runNext() // Complete asynchronous subscription creation.
      pub.sendNext(Disconnected)
      pub.expectRequest()
      f.closed.isCompleted shouldBe false
      if (cancel) sub.cancel()
      else {
        pub.sendComplete()
        sub.expectComplete()
      }
      Await.result(f.closed.future, 3.seconds)
      f.ec.expectIdle()
    }
  }

  it should "emit a batch one message at a time across reconnect without fetching ahead" in withCleanup {
    val f = new Fixture
    val a = mock[Message]
    val b = mock[Message]
    val c = mock[Message]
    val d = mock[Message]
    inSequence {
      (f.subscription.fetch(_: Int, _: Duration)).expects(100, f.timeout).returning(List(a, b, c).asJava)
      (f.subscription.fetch(_: Int, _: Duration)).expects(100, f.timeout).returning(List(d).asJava)
    }
    val (pub, sub) = f.run()
    pub.sendNext(Connected(f.connection))
    pub.expectRequest()
    f.ec.runNext() // Complete asynchronous subscription creation.
    sub.request(1)
    f.ec.runNext()
    sub.expectNext(a).expectNoMessage(100.millis)
    f.ec.expectIdle()
    pub.sendNext(Disconnected)
    pub.expectRequest()
    sub.request(1).expectNext(b)
    f.ec.expectIdle()
    pub.sendNext(Connected(f.connection))
    pub.expectRequest()
    f.ec.expectIdle()
    sub.request(1).expectNext(c)
    f.ec.expectIdle()
    sub.request(1)
    f.ec.runNext()
    sub.expectNext(d)
    pub.sendComplete()
    sub.expectComplete()
  }

  it should "drain the buffered batch before completing" in withCleanup {
    val f = new Fixture
    val a = mock[Message]
    val b = mock[Message]
    val c = mock[Message]
    (f.subscription.fetch(_: Int, _: Duration)).expects(100, f.timeout).returning(List(a, b, c).asJava)
    val (pub, sub) = f.run()
    pub.sendNext(Connected(f.connection))
    pub.expectRequest()
    f.ec.runNext() // Complete asynchronous subscription creation.
    sub.request(1)
    f.ec.runNext()
    sub.expectNext(a)
    pub.sendComplete()
    sub.request(2).expectNext(b, c).expectComplete()
    Await.result(f.closed.future, 3.seconds)
    f.ec.expectIdle()
  }

  it should "unsubscribe when cancelled with a partially emitted batch" in withCleanup {
    val f = new Fixture
    val a = mock[Message]
    val b = mock[Message]
    (f.subscription.fetch(_: Int, _: Duration)).expects(100, f.timeout).returning(List(a, b).asJava)
    val (pub, sub) = f.run()
    pub.sendNext(Connected(f.connection))
    pub.expectRequest()
    f.ec.runNext() // Complete asynchronous subscription creation.
    sub.request(1)
    f.ec.runNext()
    sub.expectNext(a)
    sub.cancel()
    Await.result(f.closed.future, 3.seconds)
    f.ec.expectIdle()
  }

  it should "reject fetch timeouts shorter than one millisecond" in withCleanup {
    for (timeout <- Seq(null, Duration.ZERO, Duration.ofMillis(-1), Duration.ofNanos(1))) {
      an[IllegalArgumentException] should be thrownBy {
        JetStreamPullSubscriptionSource("subject", 100, timeout, ExecutionContext.global,
          JetStreamOptions.defaultOptions(), PullSubscribeOptions.bind("stream", "durable"))
      }
    }
  }
}
