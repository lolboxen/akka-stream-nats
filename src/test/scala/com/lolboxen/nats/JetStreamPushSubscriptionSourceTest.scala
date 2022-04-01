package com.lolboxen.nats

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Keep}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.TestKit
import com.lolboxen.nats.ConnectionSource.{Connected, Protocol}
import io.nats.client._
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import util.MockUtils.captureAndReturn

import scala.concurrent.Promise

class JetStreamPushSubscriptionSourceTest
  extends TestKit(ActorSystem("JetStreamPushSubscriptionSource"))
    with AnyFlatSpecLike
    with Matchers
    with ScalaFutures
    with MockFactory
    with BeforeAndAfterAll {

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  it should "emit messages after connecting" in {
    val handler = Promise[MessageHandler]()
    val dispatcher = mock[Dispatcher]
    val connection = mock[Connection]
    val jetStream = mock[JetStream]
    val jetStreamSubscription = mock[JetStreamSubscription]
    val message = mock[Message]
    (connection.createDispatcher: () => Dispatcher).expects().once().returning(dispatcher)
    (connection.jetStream(_: JetStreamOptions)).expects(*).once().returning(jetStream)
    (jetStream.subscribe(_: String, _: Dispatcher, _: MessageHandler, _: Boolean, _: PushSubscribeOptions))
      .expects("subject", *, *, true, *)
      .once()
      .onCall { (_, _, mh, _, _) => captureAndReturn(handler, jetStreamSubscription)(mh) }
    (jetStreamSubscription.isActive _).expects().once().returning(true)
    (jetStreamSubscription.unsubscribe: () => Unit).expects().once()

    val (pub, sub) = TestSource[Protocol]
      .via(Flow.fromGraph(new JetStreamPushSubscriptionSource(
        "subject",
        true,
        JetStreamOptions.defaultOptions(),
        PushSubscribeOptions.bind("stream", "durable"))))
      .toMat(TestSink.probe)(Keep.both)
      .run()

    pub.sendNext(Connected(connection))
    whenReady(handler.future) { messageHandler =>
      messageHandler.onMessage(message)
      messageHandler.onMessage(message)
      sub.request(2).expectNext(message, message)
      pub.sendComplete()
      sub.expectComplete()
    }
  }

}
