package com.lolboxen.nats

import akka.actor.ActorSystem
import akka.stream.Attributes
import akka.stream.scaladsl.Keep
import akka.stream.testkit.{TestPublisher, TestSubscriber}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.TestKit
import com.lolboxen.nats.ConnectionSource.{Connected, Disconnected, Protocol}
import io.nats.client.{Connection, Message, Subscription}
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.{Await, ExecutionContext, Promise}
import scala.concurrent.duration._

class CoreSubscriptionSourceTest
  extends TestKit(ActorSystem("CoreSubscriptionSourceTest"))
    with AnyFlatSpecLike
    with Matchers
    with MockFactory
    with BeforeAndAfterAll {

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  private class ManualExecutionContext extends ExecutionContext {
    private val tasks = new LinkedBlockingQueue[Runnable]()
    override def execute(task: Runnable): Unit = { tasks.add(task); () }
    override def reportFailure(cause: Throwable): Unit = throw cause
    def pendingTasks: Int = tasks.size()
    def runNext(): Unit = {
      val task = tasks.poll(3, TimeUnit.SECONDS)
      task should not be null
      task.run()
    }
    def expectIdle(): Unit = tasks.poll(100, TimeUnit.MILLISECONDS) shouldBe null
  }

  private class Fixture(activeOnClose: Boolean = true) {
    val connection = mock[Connection]
    val subscription = mock[Subscription]
    val ec = new ManualExecutionContext
    val closed = Promise[Unit]()
    private val created = new AtomicBoolean(false)

    (connection.subscribe(_: String)).expects("subject").once().onCall { (_: String) =>
      created.set(true)
      subscription
    }
    (subscription.isActive _).expects().once().onCall { () =>
      if (!activeOnClose) closed.success(())
      activeOnClose
    }
    if (activeOnClose) {
      (subscription.unsubscribe: () => Unit).expects().once().onCall(() => { closed.success(()); () })
    }

    def read(message: Message): Unit = {
      (subscription.nextMessage(_: Long)).expects(1000L).returning(message)
    }

    def run(test: (TestPublisher.Probe[Protocol], TestSubscriber.Probe[Message]) => Unit): Unit = {
      val ((pub, stopped), sub) = TestSource[Protocol]()
        .via(CoreSubscriptionSource("subject", ec))
        .watchTermination()(Keep.both)
        .toMat(TestSink[Message]())(Keep.both)
        .withAttributes(Attributes.inputBuffer(1, 1))
        .run()
      try test(pub, sub)
      finally {
        sub.cancel()
        Await.ready(stopped, 3.seconds)
        if (created.get()) Await.result(closed.future, 3.seconds)
      }
    }

    def connect(pub: TestPublisher.Probe[Protocol]): Unit = {
      pub.sendNext(Connected(connection))
      pub.expectRequest()
      ec.runNext() // Subscription creation has its own asynchronous task.
    }
  }

  it should "subscribe on connection and read one message per downstream request" in {
    val f = new Fixture
    val first = mock[Message]
    val second = mock[Message]
    inSequence {
      f.read(first)
      f.read(second)
    }
    f.run { (pub, sub) =>
      f.connect(pub)
      f.ec.expectIdle()
      for (message <- Seq(first, second)) {
        sub.request(1)
        f.ec.runNext()
        sub.expectNext(message).expectNoMessage(100.millis)
        f.ec.expectIdle()
      }
      pub.sendComplete()
      sub.expectComplete()
    }
  }

  for (beforeConnection <- Seq(true, false)) {
    it should s"service demand received ${if (beforeConnection) "before connection" else "during subscription creation"}" in {
      val f = new Fixture
      val message = mock[Message]
      f.read(message)
      f.run { (pub, sub) =>
        if (beforeConnection) {
          sub.request(1).expectNoMessage(100.millis)
          f.ec.expectIdle()
        }
        pub.sendNext(Connected(f.connection))
        pub.expectRequest()
        if (!beforeConnection) sub.request(1).expectNoMessage(100.millis)
        f.ec.runNext() // Create the subscription.
        f.ec.runNext() // Service the demand that was already outstanding.
        sub.expectNext(message)
        f.ec.expectIdle()
        pub.sendComplete()
        sub.expectComplete()
      }
    }
  }

  it should "serialize reads when more demand and duplicate connection notifications arrive" in {
    val f = new Fixture
    val first = mock[Message]
    val second = mock[Message]
    inSequence {
      f.read(first)
      f.read(second)
    }
    f.run { (pub, sub) =>
      f.connect(pub)
      sub.request(1)
      awaitCond(f.ec.pendingTasks > 0)
      sub.request(1)
      pub.sendNext(Connected(f.connection))
      pub.expectRequest()
      f.ec.pendingTasks shouldBe 1
      f.ec.runNext()
      sub.expectNext(first)
      awaitCond(f.ec.pendingTasks > 0)
      f.ec.pendingTasks shouldBe 1
      f.ec.runNext()
      sub.expectNext(second)
      f.ec.expectIdle()
      pub.sendComplete()
      sub.expectComplete()
    }
  }

  it should "retry a timed-out read on the same subscription without satisfying demand" in {
    val f = new Fixture
    val message = mock[Message]
    inSequence {
      f.read(null)
      f.read(message)
    }
    f.run { (pub, sub) =>
      f.connect(pub)
      sub.request(1)
      f.ec.runNext()
      sub.expectNoMessage(100.millis)
      f.ec.runNext()
      sub.expectNext(message)
      f.ec.expectIdle()
      pub.sendComplete()
      sub.expectComplete()
    }
  }

  it should "pause reads during disconnect and resume using the existing subscription" in {
    val f = new Fixture
    val first = mock[Message]
    val second = mock[Message]
    inSequence {
      f.read(first)
      f.read(second)
    }
    f.run { (pub, sub) =>
      f.connect(pub)
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
      f.ec.expectIdle()
      pub.sendComplete()
      sub.expectComplete()
    }
  }

  for (completeBeforeReconnect <- Seq(true, false)) {
    it should s"retain an in-flight message completed ${if (completeBeforeReconnect) "before" else "after"} reconnect" in {
      val f = new Fixture
      val message = mock[Message]
      f.read(message)
      f.run { (pub, sub) =>
        f.connect(pub)
        sub.request(1)
        awaitCond(f.ec.pendingTasks > 0)
        pub.sendNext(Disconnected)
        pub.expectRequest()
        if (completeBeforeReconnect) {
          f.ec.runNext()
          sub.expectNext(message)
        }
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
  }

  it should "fail and unsubscribe when reading fails" in {
    val f = new Fixture
    val error = new IllegalStateException("subscription is inactive")
    (f.subscription.nextMessage(_: Long)).expects(1000L).throwing(error)
    f.run { (pub, sub) =>
      f.connect(pub)
      sub.request(1)
      f.ec.runNext()
      sub.expectError() shouldBe error
      f.ec.expectIdle()
    }
  }

  for (cancel <- Seq(true, false)) {
    it should s"unsubscribe on ${if (cancel) "cancellation" else "completion"} while disconnected" in {
      val f = new Fixture
      f.run { (pub, sub) =>
        sub.ensureSubscription()
        f.connect(pub)
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
  }

  it should "avoid unsubscribing a subscription that is already inactive" in {
    val f = new Fixture(activeOnClose = false)
    f.run { (pub, sub) =>
      sub.ensureSubscription()
      f.connect(pub)
      pub.sendComplete()
      sub.expectComplete()
    }
  }

  it should "unsubscribe when subscription creation completes after cancellation" in {
    val f = new Fixture
    f.run { (pub, sub) =>
      pub.sendNext(Connected(f.connection))
      pub.expectRequest()
      sub.cancel()
      pub.expectCancellation()
      f.ec.runNext() // The worker finishes after downstream has cancelled.
      Await.result(f.closed.future, 3.seconds)
      f.ec.expectIdle()
    }
  }
}
