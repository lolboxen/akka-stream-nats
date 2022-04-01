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

import java.time.Duration
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

class JetStreamPullSubscriptionSourceTest
  extends TestKit(ActorSystem("JetStreamPullSubscriptionSourceTest"))
    with AnyFlatSpecLike
    with Matchers
    with ScalaFutures
    with MockFactory
    with BeforeAndAfterAll {

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  it should "emit messages after connecting" in {
    val connection = mock[Connection]
    val jetStream = mock[JetStream]
    val jetStreamSubscription = mock[JetStreamSubscription]
    val message = mock[Message]
    (connection.jetStream(_: JetStreamOptions)).expects(*).once().returning(jetStream)
    (jetStream.subscribe(_: String, _: PullSubscribeOptions)).expects("subject", *).once().returning(jetStreamSubscription)
    (jetStreamSubscription.fetch(_: Int, _: Duration)).expects(100, Duration.ofMillis(10)).anyNumberOfTimes().returning(List(message).asJava)
    (jetStreamSubscription.isActive _).expects().once().returning(true)
    (jetStreamSubscription.unsubscribe: () => Unit).expects().once()

    val (pub, sub) = TestSource[Protocol]
      .via(Flow.fromGraph(new JetStreamPullSubscriptionSource(
        "subject",
        100,
        Duration.ofMillis(10),
        ExecutionContext.global,
        JetStreamOptions.defaultOptions(),
        PullSubscribeOptions.bind("stream", "durable"))))
      .toMat(TestSink.probe)(Keep.both)
      .run()

    pub.sendNext(Connected(connection))
    sub.request(2).expectNext(Seq(message), Seq(message))
    pub.sendComplete()
    sub.expectComplete()
  }
}
